# QE 统一实验登记、跨节点进度与选择性数仓 F2 详细设计

> Feature ID：`qe_unified_experiment_registry_selective_warehouse_v1`
> Feature tier：F2
> 设计版本：v1.3（保留3个交付批次；全指标/有效政策、准确正负结果入仓与有界历史修订）
> 日期：2026-09-11
> 状态：`V1_3_DESIGN_REVISED_IMPLEMENTATION_PENDING`；既有Batch A/B事实见14/15节，不代表v1.3源码或运行态已完成。
> 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.16；实验执行方案v2.0与本设计共同约束下游实验。
> 既有入仓设计：`docs/architecture/qe_archive_manual_ingestion_selection_design_20260519.md`

## 1. Background / 背景与问题

### 1.1 用户目标

所有正式QE实验计算前登记，WSL/remote执行进度、详情与历史在UI可查。全部技术准确、身份完整、非精确重复的正负结果都具备既有QE Archive入仓资格，A/B/C仅决定保存深度，不是收益门槛或仅A资格。实际写仓继续由UI人工确认或用户明确授权；无有效子结果的失败/噪声及精确重复X才可安全清理。组合效果不写成单因子绩效。

本版同步父蓝图v6.16：科创50单一选股池仅Top20；基准父loop预测复用于政策/选股池重放，训练股票池作为单独实验轴。trajectory完整显示产出指标及HMM/黑名单有效状态，不从当前配置反推历史。允许现有记录/精确引用的有界历史盘点和选择性入仓，不恢复缺失旧制品物化或通用平台。

### 1.2 初版背景与当前增量缺口

以下入口缺口为初版背景，部分已由Batch A/B修复；当前v1.3新增缺口是指标来源、有效政策、训练/选股域、准确正负结果入仓及精确清理。不得把初版问题再次全部列为当前未实现。

当前仓库不是完全缺少实验控制面：

- `POST /api/v1/quantevolver/experiments/pending` 已能在执行前创建单实验记录；
- `qe_single_experiment_create_pending` 与 `qe_experiment_run_confirmed` 已为 MCP 提供登记后执行路径；
- `/api/v1/quantevolver/experiments` 与实验详情页从 `qe_experiments` 读取历史；
- `/experiments/{id}/run-status` 已按只读原则返回持久化状态，运行态核对由后台协调器负责；
- QE Archive 已有 source-status、preview、confirmed execute、experiment/task/loop 选择和幂等写仓能力。

真实缺口是入口不统一：部分 Codex/Claude 长任务直接在 WSL 或远端执行 qrun，任务 manifest 会明确记录 `backend_used=false`、`ui_used=false`、`scheduler_used=false`。这类任务虽然生成了 workspace、配置和回测结果，却没有 `qe_experiments`、task/loop 或 Archive source identity，UI 自然无法发现。部分 Multi-Alpha 路径也在装配后才持久化 parent，尚未形成“登记成功后才允许分发”的统一不变量。

QE Archive 的现行手动入仓原则是正确的：数仓不应自动保存每个大型 workspace。v1.1 曾把“所有正式终态长期保留轻量运行历史”作为统一可观察性的组成部分；用户在 2026-09-11 明确修订该规则：运行中和刚形成终态的可观察性继续保留，但失败、无有效结果和精确重复实验不再作为长期研究历史。技术准确且唯一的结果进入价值分级，包括负收益；失败诊断如需长期跟踪进入独立BUG/Issue。父任务失败不决定有效child的资格。

### 1.3 设计结论

本功能不建立新调度平台，不建立第二套数仓，也不扫描历史 workspace。它只在现有 QE 创建、执行、状态、UI 和 Archive 能力之间建立一条强制的正式实验合同：

```text
人类可读实验请求
  -> 现有 QE 控制面预登记并生成 canonical identity
  -> 冻结数据 release / universe / model / factors / seed / TWAP 合同
  -> 现有 WSL 或 remote dispatcher
  -> 现有节点状态与 artifact receipt
  -> 按变化收敛到 qe_experiments / task / loop
  -> UI 展示在途对象和成功、唯一的长期历史
  -> 失败/无结果/精确重复经安全分类后清理
  -> 准确非重复正负A/B/C按深度保存；用户UI或明确授权后选择性写同一QE Archive
```

## 2. Scope / 范围

### 2.1 Goals / 目标

1. 为所有正式 QE 单实验、自动演进、自定义演进、策略演进、多 Alpha parent/child、单腿刷新和正式组合回测提供同一登记优先合同。
2. UI、Backend/Scheduler、Codex MCP、Claude Code MCP 最终复用同一控制面身份和执行入口。
3. WSL 与远端节点在计算开始前取得已持久化的 experiment/task/loop identity，不允许先运行后补身份。
4. UI 能以日期、类型、状态、节点、模型、因子族、股票池、数据 release 和执行算法搜索全部正式实验，无需人工输入内部 ID 或 JSON。
5. completed 且非精确重复的实验保留轻量历史；父任务 partial/failed 下的成功 child 不得被父状态覆盖。failed 及被成功 retry 取代的失败 attempt 在结构化根因已投递 BUG/日志后进入清理候选，不作为长期实验样本。
6. Backend 重启不终止已分发的 WSL/远端计算；恢复后通过既有协调器和节点回执幂等收敛，不重复提交。
7. 成功、非重复实验按 A/B/C 三档保存：A 为完整策略锚点，B 为紧凑有效研究，C 为最小成功摘要；失败、无有效结果、验证噪声和精确重复为 X 档并删除。
8. 普通 workspace cleanup 不删除 A/B/C 控制面历史；X清理须覆盖控制面/数仓/精确制品引用，按4.8显式展示部分失败与真实artifact状态，禁止假原子承诺和伪成功。
9. 正常未变化心跳不追加事件；UI 未打开不产生前端轮询；节点探测与状态核对不短于 60 秒。
10. 训练、预测、回测数据面继续只读版本化 Bin/H5/Parquet/sidecar，数据库只承担控制面和结果面。

### 2.2 正式实验定义

只要一次执行的结果会参与 Alpha、模型、因子、股票池、策略、成本、执行算法或策略包比较，就属于正式实验，必须在计算前登记。登记保证运行时可观察性，不等于永久保存资格：成功且非重复的 trial 才进入 A/B/C 长期历史；失败、无有效结果和精确重复在终态分类后进入 X 档清理。结果弱但成功且唯一的 trial 不是失败，可作为负对照或最小研究摘要保留。

单元测试、fixture、开发期极小 smoke 可以不进入研究历史，但必须显式标记 `purpose=validation`，不得被报告为正式实验结论。任何生成可用于研究比较的完整预测或组合指标的运行，不得借 `smoke` 名义绕过登记。

### 2.3 Non-Goals / 非目标与边界

- 允许§3.6有界盘点既有记录/精确引用及可核验结果选择性入仓；不扫描全部历史workspace、不重建过去缺失制品、不制造未知指标或历史identity。
- 不自动把全部实验、日志、模型权重和 workspace 复制进 QE Archive。
- 不建立新的通用任务平台、消息总线、事件仓库、心跳表、监控 daemon 或配置中心。
- 除用户明确要求的未来科创50单一池Top20，及执行方案预注册的政策/股票池实验轴外，不擅改模型、因子、label、seed、切分、成本或分钟TWAP语义；历史配置不覆盖。
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
| UI 历史 | `/quantevolver/experiments`、evolution/task/loop 详情与结果接口 | 搜索、分组、在途进度、A/B/C 长期结果、价值等级与 X 清理预览 | 扫描节点目录发现实验、写状态、把 X 档长期伪装成研究样本 |
| 长期数仓 | `qe_archive` schema、backfill preview/execute、source-status | 保存用户选择或明确授权的准确非重复A/B/C正负结果；档位只决定制品深度 | 自动全量入仓、替代运行控制面、保存失败或精确重复实验 |

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

### 3.5 A/B/C/X 保留深度与入仓资格

正式运行期保存identity/parent-child、来源/purpose、完整模型/因子/seed/标签/切分/训练域/选股域/政策/分钟执行摘要、release/节点、阶段时间、metrics与精确artifact引用。终态逐最小可评价experiment/child/loop分类：

| 级别 | 定义 | 保存深度及Archive资格 |
|---|---|---|
| A | 准确非重复的策略锚点、重要matched对照、用户指定样本 | 结构化结果、可复算曲线及必要模型/预测/成交等资产引用；可人工/授权入仓 |
| B | 准确非重复的有效研究结果，包括负收益/负对照、跨seed/时期/池代表 | 完整研究配置/身份、指标、必要曲线/归因摘要与可用引用；同样可人工/授权入仓 |
| C | 准确非重复、增量有限但仍可作为比较依据 | 最小完整身份/配置/指标/口径与retention摘要；同样可人工/授权入仓，无需升A |
| X | 无有效子结果的失败/取消、无结果噪声、可删除validation，或已证明精确重复 | 故障根因及聚合试验计数保留到既有研究/BUG记录；精确授权清理后不留无用实验制品/历史 |

盈利、CAGR、IC不能决定技术有效性；缺失身份/结果/保护引用证据标classification_pending，保持可见且不删除。父failed/partial逐child分类，保留有效child及其必要父身份容器；不得整体删父或将无结果失败伪装负Alpha。旧日频/V25可以保留其准确signal或诊断证据，明确portfolio validity，不升格成分钟真值；混合有效性结果按可证实的层入仓，未知层不伪造。

精确重复须**同时**：输入release/组件/源码/模型及拟合处理器、训练/推理/交易股票池PIT、特征/label/seed/切分、窗口/benchmark/TopK/政策HMM/黑名单及生效域/费用/分钟执行合同一致，且权威结果内容digest一致。prediction相同不是结果相同；相同prediction不同政策/池是正式对照。retry lineage不是重复证明；相同配置结果不同保留并诊断确定性，不能自动删。hash仅对规范化业务内容比较，时间戳等非业务差异不得掩盖真实不同结果。

使用现有Archive preview/confirmed execute及experiment/loop粒度幂等。A/B/C只决定复制/引用哪些深层制品，不新建第二数仓；原始完整workspace一律不自动全拷贝。规则推荐不自动写仓，UI/用户明确授权后可批量选择任意准确A/B/C。技术无效项不得用“手动选择”绕过。

### 3.6 有界历史盘点、入仓与X清理

v1.3替代早期“全部历史不处理”：按既有QE/Archive分页及固定截止游标盘点，先近六个月、旧优胜候选及已被引用结果，再其余记录；显示总范围、已核查与未核查数量。只沿精确已记录artifact引用读取，不扫描整个节点目录、不重建缺失历史文件。已知孤儿run仅在用户点名且配置/结果/路径可证实后预览补录，不伪造创建时登记。

先分日频、退役V25、有效TWAP、仅signal有效；按3.5生成A/B/C入仓候选和X preview。全历史入仓/清理不是新实验前置，已确认单腿可以先运行。本版只授权设计，实际Archive写入与删除仍按明确授权/既有DEV和readback规则。

删除X前在现有研究批次摘要保留搜索次数、候选家族/协议、失败原因计数与去重映射，不能因清理减少多重检验试验数。有效负结果不能删来美化收益。A/B/C需要复用的共享prediction/权重、权威receipt及下游引用受保护；B/C最小摘要不代表所有大文件都可删。

## 4. Contracts / API、DB、UI 与 MCP 契约

### 4.1 Canonical identity 与配置摘要

控制面在计算开始前生成 identity；节点不得自造第二 identity。公开 UI/MCP 接受模型、因子、日期、股票池、节点等语义字段，server 负责生成内部 identity 与 run-scoped binding。

每个正式 run 必须固化以下摘要：

| 类别 | 必须字段 |
|---|---|
| 来源 | source type、created-by type/name、purpose、parent/task/loop 关系 |
| 数据 | release/profile generation/cutoff、节点与组件identity、train/inference/selection/quote universe分别记录、PIT规则、benchmark |
| 研究 | 模型/拟合处理器/特征与源码digest、seed、label/split、训练池、prediction来源父loop/hash及覆盖；组合表现不能作为成员单因子绩效 |
| 执行 | execution algo/1min、目标TopK及实际持仓、n_drop/动态参数、费用/停牌/涨跌停；HMM/黑名单requested/enabled/effective/trigger_count、版本/生效区间/因果输入来源 |
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
| failed | 结构化失败；在故障投递与安全分类完成前可见，随后进入 X 清理；成功 sibling 不被覆盖 |
| cancelled | 已确认取消；未知远端状态不得冒充 cancelled；无有效结果者进入 X 清理 |
| interrupted | 节点或进程中断有直接证据，可按既有 retry 语义恢复 |
| reconciling | Backend 恢复后正在核对远端；不是失败 |

父 task/experiment 同时展示 child 总数和 planned/queued/running/completed/failed/cancelled 数量。父级 failed/partial 不得让已完成 child 从 UI 或数仓候选中消失；X child 清理后父级计数与聚合状态必须在同一操作中重算，禁止留下悬空引用。

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
- archived/recommended/not-archived；
- value tier（A/B/C）与 cleanup candidate（X，只在清理前短期可见）。

`detail=summary` 不返回大 JSONB；详情页按需读取配置摘要、指标、日志 tail、child grid 与 archive status。API 输出人类可读 display fields，并保留内部 identity 供页面路由，不要求用户复制粘贴。

### 4.5 UI 与指标权威合同

正式历史入口沿用现有实验列表、evolution trajectory和Archive服务，日期/模型/池/状态/节点/用途/政策/入仓状态业务筛选，无人工ID/JSON。所有在途对象可见；A/B/C长期可读；X清理前可见原因、清理中显示真实进度而不是伪装完整结果。日志仅显式点击读取。

trajectory **每个loop的全部已产出指标**均可在普通表格列组内查询：关键项默认显示，其余横向滚动/展开/列选择，不仅放详情或输出JSON。缺字段显示“未记录/不可计算/标签未成熟”等reason与样本数，不编造0。

| 列组 | 必须可展示的实际字段 |
|---|---|
| 实验/样本 | 模型/seed/因子集合及实际特征维数、训练池、推理/选股池及单选/并集PIT、数据release/cutoff、requested/实际预测/成交/标签窗口、耗时/状态/节点、父预测来源 |
| 策略 | TWAP版本/频率、目标TopK/n_drop及动态规则、实际持仓/滞留/未成交、HMM/黑名单requested/enabled/effective、政策类型/版本/覆盖/触发次数 |
| 绝对净值 | 累计、CAGR、Sharpe、绝对MDD、Calmar、波动率 |
| 基准/主动 | 公共和池benchmark名称/窗口/累计/年化、扣费主动年化、tracking error、IR、相对财富；beta或Brinson已有可靠数据则显示，否则不可计算 |
| 信号 | IC/ICIR、RankIC/RankICIR、Top20/50标签收益和命中、decay、dispersion、交易日/标签成熟覆盖；纯科创50以Top20为主，Top50标不适用 |
| 交易 | 换手、费用、成本前后收益、成交/未成交率、实际暴露、订单/资金规模等已产出诊断及其单位 |
| 保留 | A/B/C/X、技术有效性/协议、入仓状态、artifact available/cleaned/missing及原因 |

绝对指标来自同一权威净值（现有absolute_returns字段需与净值核对）；CAGR与MDD必须同源、Calmar不混绝对/超额MDD。Sharpe注明无风险、ddof、年化N；IR由同日期扣费主动日收益及tracking error计算，不能Sharpe兜底。benchmark必须同日期真实序列；算术主动年化与几何相对财富分别命名，禁止两个年化相减伪造主动收益。既有1day.excess_return_with_cost字段须确认费用从现金账扣除且不重复扣，不能只按键名认定正确。保留metric source、单位、N、频率、样本窗口/日数与缺失原因的轻量投影，不建设第二指标平台。

2026-09-11只读审计发现列表HMM与comparison不一致、黑名单详情可能重构当前配置、max_drawdown混义及IR/Calmar回退风险。实施必须读最终运行配置/冻结policy及产物，缺证据显示unknown，不把当前配置或摘要默认值当历史实际生效。HMM开但0触发与未开不同；enabled不等于收益提高。七八月/全窗/标签成熟分开；实际持仓超TopK解释不可成交滞留，不强行改结果。

未来纯科创50创建、retry/clone和所有政策/训练池变体统一Top20：默认20、显式非20拒绝，后端消费校验与UI/MCP一致；更大多池PIT并集不误判。历史Top50只读旧协议，不覆盖。列表训练池与选股池分开，能辨别“只改选股池零训练”和“池内另训”，相同prediction不同政策不显示为重复。

HMM entry gate若保护Top30覆盖科创50全部Top20候选，应显示enabled但no-action并保留触发0，不将无作用误判为HMM效果；具体预注册政策按执行方案固定，不能事后调参。

因子库参考仅展示该组合涉及的因子、模型/池/时间/政策及结果引用，标“组合观察证据”；单因子官方IC、条件加入/移除增量、组合收益三个层级不混用，QE不写因子评分/晋升。

### 4.6 MCP 与 Codex/Claude 契约

- 继续复用 `qe_experiment` 和 `qe_archive` MCP module，不新增第二个 MCP server。
- 创建工具返回 identity 和人类可读摘要；运行工具只接受已登记 identity。
- 增强 list 工具的业务筛选，使 Codex/Claude 可以先按日期、模型、task name 等查询，再内部选择 identity，用户无需提供 ID。
- 入仓工具继续区分 preview 与 confirmed execute。只有用户本轮明确授权后客户端才调用 execute；查看推荐和 preview 为只读。
- MCP 不允许客户端提交 server-owned provider path、dataset binding、hash 或 workspace path。
- MCP 客户端同步、source merge、Backend 重启与实验执行分别报告，不互相推导。

### 4.7 高价值实验推荐规则

系统只计算 `recommended`、`not_recommended` 或 `manual_only` 及原因，不自动写仓。

按可评价child/loop判断技术准确、身份完整、非精确重复；父级failed不取消有效child资格，负收益不取消资格。默认推荐与Archive资格不同。优先推荐：

- 当前冠军、Pareto 前沿或策略包候选；
- matched comparison 的代表性 baseline/variant；
- 新 Alpha、模型、股票池、data release 或执行算法的正式基线；
- 多 seed、跨 vintage、LOO、成本容量、regime/staleness、右尾捕获中的代表样本；
- 决定性负对照或能够证伪研究假设的成功实验。

默认不推荐：

- purpose=validation 的 smoke/fixture；
- 精确重复 run；
- failed/cancelled 且无有效结果、被成功 retry 取代的失败 attempt；
- 缺失 canonical config、dataset identity 或终态证据的异常运行；
- 日频 `CLOSE_PRICE` companion；
- 缺分钟 TWAP、费用、停牌/涨跌停或可复现制品的收益结果。

用户可以直接选择技术准确、身份完整的B/C样本入同一Archive，无需升为A；manual_only也不能绕过schema、identity、配置完整性和幂等约束。失败的技术完整性检查必须 loud fail；若其根因值得保留，投递独立 BUG/Issue，而不是保留失败实验。

### 4.8 清理与保留契约

- running、planned、queued、finalizing、reconciling、paused 或远端状态 unknown 的对象全部受保护，不进入 X 分类。
- A/B/C 的控制面摘要长期保留；其 workspace、日志、缓存和模型文件按价值等级及现有有界 retention/精确 manifest 清理，清理只更新 artifact retention。
- X 清理必须以精确 experiment/task/loop identity 和 artifact manifest 为输入，先生成只读 preview，验证没有 tracked file、symlink/junction、活动进程引用、Archive/冠军/策略包/下游任务等受保护引用，再执行节点制品清理与数据库事务。任一引用不确定即 fail closed。
- X清理不是跨文件系统/DB原子事务：先按精确manifest登记清理中状态并保护survivor/共享引用，再逐文件取得节点回执，全部完成后才事务删除目标数仓/控制记录。部分失败保留cleanup_incomplete和逐文件available/deleted/unknown状态，UI明确显示并禁用已删文件下载，不冒充完整结果或删除成功；安全重试不得重复删共享资产。
- 对 exact duplicate 先确定 canonical survivor：优先已入仓/已被引用者，其次终态证据完整且最早成功者；只删除重复副本。相同配置但 digest 不同不得自动去重。
- partial/failed parent逐child清理，保留有效sibling及必要父容器；仅更新retention/清理计数投影，不将原failed状态改写completed或重算原实验收益。
- 已入仓 artifact 由 QE Archive 自身 manifest 与 retention 管理；除非明确取消保护并经用户授权，不进入自动 X 清理。
- 不允许通配符、目录级自动发现、递归删除，或数据库记录与文件目录的双向猜测。

## 5. Design Acceptance Index / 设计验收索引

| 编号 | 设计要求 |
|---|---|
| F-001 | 所有正式 QE 入口在计算前持久化 canonical identity，登记失败不得分发 |
| F-002 | 单实验、全部 evolution 类型和 Multi-Alpha parent/child 使用同一登记适配器语义 |
| F-003 | Codex/Claude 正式实验只走 QE MCP/控制面，不再裸跑无登记 qrun |
| F-004 | WSL 与远端使用同一逻辑身份和 release，node-specific path 不泄漏到另一节点 |
| F-005 | Backend 重启不终止或重复分发在途实验，恢复后状态幂等收敛 |
| F-006 | 所有正式实验执行期间可见；终态后只保留 A/B/C，X 经安全清理删除；partial/failed parent 下的成功 child 必须保留 |
| F-007 | UI无人工ID/JSON；trajectory每loop全产出指标可查，训练/选股池分开，HMM/黑名单effective与缺失原因准确 |
| F-008 | GET、UI 关闭和未变化心跳不产生无意义 DB 写入，远端检查不高于每对象每 60 秒 |
| F-009 | 注册/最终配置绑定模型/预测/特征/数据/训练池/选股池/政策/费用；纯科创50未来仅Top20，历史不反推/改写 |
| F-010 | QE 数据面继续只读 Bin/H5/Parquet/sidecar，数据库仅用于控制面和结果面 |
| F-011 | 全部技术准确、身份完整、非重复正负A/B/C均可人工/授权入同一Archive，无需升A；分级仅决定深度 |
| F-012 | Archive 写入复用现有 preview/confirmed execute、source-status 和 experiment/loop 粒度幂等语义 |
| F-013 | 推荐包含有效负对照，组合结果仅为因子参与参考，不把组合收益/IC写成单因子绩效 |
| F-014 | A/B/C workspace cleanup 只更新 artifact retention；X 以精确 manifest 跨节点与 DB 一致清理，受保护引用 fail closed |
| F-015 | 有界盘点现有记录/精确引用，报告覆盖与未知；不物化缺失旧制品，不让全历史工作阻断新候选 |
| F-016 | 不新增通用平台、第二数仓、事件仓、心跳表、daemon 或非必要审批 |
| F-017 | WSL/remote、成功与 X 清理、partial parent、Backend 重启恢复、UI 股票池/收益字段与授权 MCP 入仓均有真实验收 |
| F-018 | 代码合入、运行时生效、客户端同步、数仓写入和实验启动保持独立状态 |
| F-019 | 实施必须逐项满足 DESIGN-COMPLIANCE-001，不得以部分入口或 mock-only 冒充完成 |

## 6. Implementation Plan / 三个交付批次

本节只定义 3 个真实交付批次，不再把入口清单、测试准备或发布验收拆成独立开发阶段。默认每个批次形成一个可独立审核的 source PR 或验收 receipt；只有真实 ownership 冲突或 CI 无法安全覆盖时才允许在同一批次内技术性拆分 PR，拆分不产生新的业务审批、研究门禁或串行等待。19 项 Design Acceptance Index 保持不变。

| 批次 | 默认交付物 | 主要验收项 | 唯一结束标识 |
|---|---|---|---|
| A：统一登记与状态收敛 | 1 个 Backend/MCP/最小 Frontend source PR；必要运行态 readback 独立记录 | F-001～F-006、F-007 最小可见性、F-008～F-010、F-015～F-016 | `BATCH_A_REGISTERED_RUNTIME_READY`：所有正式入口先登记、现有 UI/API 可发现最小记录与进度、双节点状态可收敛；若需用户重启则在重启前保持 pending |
| B：统一历史 UI、价值分级与选择性入仓 | 既有 v1.1 source 加 1 个最小 v1.3 Backend/Frontend 增量 PR | F-007 完整业务体验、F-011～F-014，并回归 A 的 identity/read-only 合同 | `BATCH_B_VALUE_RETENTION_RUNTIME_READY`：业务筛选、trajectory 可比字段、A/B/C/X、精确清理与 preview/confirmed Archive 运行态可用 |
| C：真实验收与上线 | 1 份绑定最终 HEAD 的验收 receipt；默认无 source PR | F-017～F-019，并汇总 F-001～F-016 | `FEATURE_RUNTIME_VERIFIED`：WSL/remote、用户重启恢复、UI、X 清理与授权 MCP 入仓全部形成真实证据 |

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

### Delivery Batch B：统一历史 UI、价值分级与选择性入仓

目标：在 Batch A 的唯一 identity/状态源上保留执行期可观察性，长期只展示 A/B/C，并交付用户选择或明确授权后的既有 QE Archive 入仓，以及失败/无结果/validation/精确重复 X 档的安全一致清理。

1. 扩展现有实验列表 API 的服务端分页和业务筛选；复用实验/evolution/Multi-Alpha 详情组件，统一显示状态、progress counts、数据 release、股票池、模型、因子、seed、节点、分钟 TWAP、价值等级和 artifact retention。
2. 禁止内部 ID/JSON 作为普通用户主流程；以日期、类型、状态、节点、模型、因子族、股票池、release 和执行算法完成搜索、展开和选择。
3. 复用 source-status、backfill preview/execute 与既有 Archive assembler；UI 支持 experiment/task/loop 多选、价值等级、推荐原因、preview 和 confirm。
4. 扩展 MCP 业务筛选和用户明确授权后的 confirmed execute；推荐规则只生成候选，不自动入仓。
5. 在 trajectory 每个 loop 明示 run-scoped 股票池与 benchmark，并把绝对 CAGR、benchmark 年化收益、扣费超额年化收益和 IR 分列；缺少权威来源显示不可计算，不做字段猜测。
6. 实现 A/B/C/X 分类、精确重复 canonical survivor、partial parent child 级清理、受保护引用 fail-closed、跨节点制品与 DB 一致清理；不引入通用 GC、daemon 或新数仓。
7. 覆盖重复入仓幂等、部分入仓、X 清理幂等、跨节点部分失败、已清理 workspace、artifact cleaned 和 A/B/C 长期摘要语义。
8. 完成本批 Backend/Frontend/MCP 聚焦测试、可访问性与业务流 CI；运行态激活仍按 changed-files 合同分别报告，不与 source merge 合并。

### Delivery Batch C：真实验收、上线与策略主线持续运行

目标：不默认新增业务代码，以最终合入 HEAD 完成真实双节点、重启恢复、UI 和选择性入仓验收；发现代码缺陷时按独立 BUG 修复，不把验收批次扩张成第四个 Feature 阶段。

1. 确认 Batch A/B 的 source、CI、运行态身份和客户端状态分别完成；任何未完成状态单列，不伪造整体验收。
2. WSL 与 remote 各运行一条正式短实验，验证分发前 UI/控制面已有记录、状态完整收敛、provider binding 正确且不突破现有并行上限。
3. 由用户执行一次 Backend 重启，验证外部节点任务继续、恢复后 canonical identity 不变、无重复 attempt、GET 仍只读。
4. UI 和用户明确授权的 MCP 各完成一次选择性入仓，验证 preview、confirmed execute、source-status、幂等和 A/B/C 摘要在 workspace cleanup 后仍可读。
5. 对一条失败无结果、一条精确重复和一个 partial parent 做 X 清理验收，确认成功 sibling 保留、UI/DB/数仓/双节点制品一致且保护引用会 fail closed。
6. 汇总 19 项实现证据与 DESIGN-COMPLIANCE-001 四项，形成最终 receipt；不启动批量历史补账、全量 artifact 复制或通用平台建设。
7. 策略主线在 Batch A 验证后已经恢复；Batch C 完成只代表本 Feature 全量验收，不是 LSTM 多 seed、现实两层板块/lead-lag 或右尾实验的启动门禁。

## 7. Verification Plan / 验证方案

### 7.1 代码和合同测试

- `backend/tests/quantevolver/test_qe_registered_submission.py`：各入口 reservation-before-dispatch、登记失败不执行、source/purpose 摘要。
- `backend/tests/quantevolver/test_qe_experiment_history_contract.py`：服务端筛选、parent-child、A/B/C/X 分类、失败/取消/partial、artifact retention。
- `backend/tests/quantevolver/test_qe_reconciliation_coordinator.py`：60 秒节流、变化才写、重启后幂等、不重提交。
- `backend/tests/multi_alpha/test_durable_orchestrator_restart.py`：Multi-Alpha parent/child 先登记、restart recovery。
- `backend/tests/qe_archive/test_manual_ingestion_selection.py`：preview/confirmed execute、loop 粒度、部分/全部入仓、幂等。
- `backend/tests/test_aistock_qe_mcp_servers.py`：业务筛选、create/run、用户授权入仓与 server-owned 字段拒绝。
- `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts`：业务筛选、进度、详情、无 ID/JSON、价值等级、Archive preview/confirm。
- `frontend/tests/quantevolver/qe_evolution_trajectory_metrics.spec.ts`（实现时新增或映射到现有同职责测试）：loop 实际股票池、legacy 未记录、绝对 CAGR、benchmark 年化、扣费超额年化、IR 与 IC/RankIC 的字段语义。
- X 清理聚焦测试：精确重复 canonical survivor、不同 seed/universe 不误删、相同配置但不同 digest 阻断、partial parent 成功 sibling 保留、受保护引用 fail closed、跨节点部分失败可重试、DB/文件一致回读。

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
| failed/cancelled | 清理前终态、阶段和 reason code 可见；故障投递后 X 被一致删除，成功 sibling 保留 |
| Backend 重启 | 用户重启期间节点继续，恢复后 identity 不变、无重复 attempt |
| UI 入仓 | 业务筛选后勾选、preview、确认、source-status=archived |
| 授权 MCP 入仓 | 用户明确授权后 confirmed execute，重复执行不产生重复 run |
| trajectory 可比性 | 每个 loop 回读实际股票池/benchmark；绝对 CAGR、benchmark 年化、扣费超额年化和 IR 不混义 |
| A/B/C workspace cleanup | 长期摘要仍可读，artifact 标记 cleaned |
| X cleanup | 精确对象在 UI/控制面/数仓/双节点制品一致消失；受保护引用和不确定状态 fail closed |

Broad UI/API/business-flow 可委托 Validation Center；最终 receipt 必须绑定实现分支最终 HEAD。mock 或静态检查只能证明局部合同，不能替代双节点和 UI 运行态验收。

## 8. Design Acceptance Matrix / 设计验收矩阵

本版DESIGN_REVIEW_READY仅指设计条款审核，gap“无”表示无设计遗漏；不宣称新增源码/运行态验收。既有batch_*是对应旧版本历史证据；本版实际实现待办见§16。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/quantevolver/qe_run_registry.py`; single/evolution/Multi-Alpha/durable 调用点 | `backend/tests/quantevolver/test_qe_registered_submission.py` | batch_a_source_test_pass | none |
| F-002 | `qe_run_registry.py`、现有 evolution/durable adapters | `backend/tests/quantevolver/test_qe_registered_submission.py` | batch_a_source_test_pass | none |
| F-003 | `backend/mcp/modules/qe_experiment.py`; `backend/services/qe_templates/materializer.py` | `backend/tests/mcp/test_domain_modules.py`; `backend/tests/test_aistock_qe_mcp_servers.py` | batch_a_source_test_pass | none |
| F-004 | `qe_run_registry.py` portable registration；active dataset binding | `backend/tests/quantevolver/test_qe_registered_submission.py`; `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | batch_a_source_test_pass | none |
| F-005 | 现有 reconciliation coordinator 与 durable readback adapter | `backend/tests/quantevolver/test_qe_reconciliation_coordinator.py`; `backend/tests/multi_alpha/test_durable_orchestrator_restart.py` | batch_a_source_test_pass | none |
| F-006 | `qe_run_registry.py::project_history`；v1.3 价值分类/清理 service 待实现 | `backend/tests/quantevolver/test_qe_experiment_history_contract.py`; `backend/tests/quantevolver/test_qe_value_retention_cleanup.py` | DESIGN_REVIEW_READY | none |
| F-007 | 本文3.5/3.6/4.1/4.5；父蓝图9.10；执行方案v2.0 | validation-receipt: v1.3跨文档合同及反例审核；§7后续业务验收 | DESIGN_REVIEW_READY | 无 |
| F-008 | persisted-only GET；60 秒 coordinator；可见页 30 秒 fallback；隐藏页零轮询 | `backend/tests/quantevolver/test_qe_reconciliation_coordinator.py`; `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts` | batch_a_source_ready_for_ci | none |
| F-009 | 本文3.5/3.6/4.1/4.5；父蓝图9.10；执行方案v2.0 | validation-receipt: v1.3跨文档合同及反例审核；§7后续业务验收 | DESIGN_REVIEW_READY | 无 |
| F-010 | 控制面登记不进入 qrun 数据面；既有 subprocess DB 隔离保持 | `backend/tests/multi_alpha/test_qe_subprocess_db_isolation.py` | batch_a_source_test_pass | none |
| F-011 | 本文3.5/3.6/4.1/4.5；父蓝图9.10；执行方案v2.0 | validation-receipt: v1.3跨文档合同及反例审核；§7后续业务验收 | DESIGN_REVIEW_READY | 无 |
| F-012 | existing `qe_archive` source-status/backfill service and list-page preview/execute | `pytest -q backend/tests/qe_archive/test_manual_ingestion_selection.py`; `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts`；真实幂等写仓验收归 Batch C | batch_b_source_local_test_pass | none |
| F-013 | 本文3.5/3.6/4.1/4.5；父蓝图9.10；执行方案v2.0 | validation-receipt: v1.3跨文档合同及反例审核；§7后续业务验收 | DESIGN_REVIEW_READY | 无 |
| F-014 | existing registered artifact cleanup；X exact cleanup 与保护引用待实现 | `backend/tests/unified_engine/test_qe_cleanup_path_policy.py`; `backend/tests/quantevolver/test_qe_value_retention_cleanup.py`；运行态 cleanup 验收归 Batch C | DESIGN_REVIEW_READY | none |
| F-015 | 本文3.5/3.6/4.1/4.5；父蓝图9.10；执行方案v2.0 | validation-receipt: v1.3跨文档合同及反例审核；§7后续业务验收 | DESIGN_REVIEW_READY | 无 |
| F-016 | 仅复用现有 QE 表、接口、coordinator 与 bounded log | `python -m nox -s l0` | batch_a_source_ready_for_ci | none |
| F-017 | §7.3 | `frontend/tests/quantevolver/qe_evolution_trajectory_metrics.spec.ts`; validation-receipt: dual-node-restart-archive-and-x-cleanup-e2e | DESIGN_REVIEW_READY | none |
| F-018 | §2.3、§9 | `python -m nox -s validation_module_registry_l0` | design_review_pass | none |
| F-019 | §11 | validation-receipt: F2 feature validator PASS；DESIGN-COMPLIANCE-001 四项逐项审核 | design_review_pass | none |

## 9. Rollout / Rollback / 发布与回滚

### 9.1 发布顺序

1. 合入 v1.3 设计增量；文档不产生运行时影响，v1.1 已合入事实保持。
2. Delivery Batch A 一次交付登记与状态收敛；测试、CI、source merge、用户重启和 post-restart readback 分开记录。
3. Batch A 运行态验证后，新正式实验立即走 registered path，策略主线无需等待 Batch B/C。
4. Delivery Batch B 以最小增量交付价值分级、X 精确清理、trajectory 股票池/收益可比字段；若 changed files 推断多个 runtime target，分别报告激活状态。
5. Delivery Batch C 以最终 HEAD 完成 WSL/remote、用户重启恢复、UI、X 清理与授权 MCP 的真实验收；默认不新增代码。

### 9.2 回滚

- 文档异常：revert 文档 merge commit，不影响实验和数据。
- source 异常：revert 对应 source merge，用户按 runtime contract 决定重启；回滚前停止新的 X apply，只允许读取已生成 receipt 并完成一致性恢复。
- 状态协调异常：停止新的统一入口写入，保留节点在途任务；恢复旧读取能力但禁止新增未登记正式任务。
- Archive 入口异常：关闭新的选择入口，保留已入仓 run 和 A/B/C 摘要；禁止通过删除数仓数据回滚。
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
| cleanup/delete | noop | 本设计 PR 不删除任何数据；后续 X apply 属独立生产 DML/文件删除动作，必须先 DEV 验证、精确 preview、用户授权与 readback |

## 11. Risks / Failure Modes / 风险与失败模式

| 风险 | 设计处理 | 禁止做法 |
|---|---|---|
| 直接 qrun 继续产生孤儿实验 | 正式入口 inventory + MCP/Backend registered path + test guard | 运行后手工拼 DB 记录 |
| 登记成功但分发失败 | 故障处置前保留 reason code；投递 BUG/Issue 后按 X 一致清理 | 未投递诊断就删除，或把失败长期当研究样本 |
| 先分发后登记 | reservation readback 是分发前技术不变量 | 异步“以后再补身份” |
| Backend 重启重复提交 | remote identity、attempt fencing、CAS reconciliation | 看到 running 就无条件重新启动 |
| UI GET 触发 DB/节点写入 | persisted read-only endpoint + background owner | 每次刷新都 reconcile |
| 高频健康检查膨胀事件表 | 60 秒节流、未变化不写、终态退出 | 2 秒轮询和每次 heartbeat append |
| 所有run自动归档造成磁盘膨胀 | A/B/C/X分层，准确A/B/C可授权入仓但制品深度受控 | 复制全部 workspace |
| 删除失败导致研究偏差 | 收益矩阵只含可评价结果，DSR/PBO搜索次数保留实际候选/协议与失败计数；有效负结果保留，X根因进入BUG | 把无收益指标的 failed 当成策略表现，或删除成功负对照 |
| X 清理造成半状态 | 精确 manifest、跨节点回执、保护引用、先制品后 DB 与可重试 `cleanup_incomplete` | DB/file 猜测、宽泛级联或伪成功 |
| A/B/C workspace 清理后 UI 404 | 长期摘要与 artifact retention 分离 | 删除 A/B/C 控制面摘要 |
| 人工 ID/JSON 不可用 | 业务筛选、勾选和内部 identity 路由 | 暴露 ID 输入作为主流程 |
| 双节点混用 release/path | run-scoped binding + node resolver + hash identity | WSL path 发送远端或静默回落 |
| 功能挤占策略主线 | Batch A 运行态验证后恢复 registered 正式实验，Batch B/C与实验并行；仅3.6有界历史、不物化缺失资产 | 等全平台完善才允许运行 Alpha |
| 非 QE 范围扩张 | QE-only ownership 和 changed-files scope | 顺带修改荐股、模拟盘或生产交易 |

## 12. DESIGN-COMPLIANCE-001 / 四项逐项审核

| 检查项 | 结论 | 直接依据 |
|---|---|---|
| 禁止简化版、子集、POC、占位或 mock-only 冒充完成 | PASS（设计） | 单/多 Alpha、全部 evolution、UI/MCP、WSL/remote、A/B/C/X、partial parent、重启恢复、选择性入仓和精确清理均进入 19 项验收；最终还要求真实双节点和 UI 证据 |
| 禁止静默错误或伪成功 | PASS（设计） | 登记失败不分发；未知远端不伪造终态；相同配置不同 digest 不误删；保护引用与跨节点部分失败 loud fail；收益缺权威来源显示不可计算 |
| 禁止未经确认改变业务逻辑 | PASS（设计） | 用户明确要求科创50Top20与准确正负结果入仓；其余模型/seed/label/费用/TWAP保持预注册语义，历史不覆盖 |
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
| Review-21 | Playwright 失败反馈预算 | 关键交互继承 30 分钟全局测试上限，合同偏差会长期占用自托管 runner | 两个关键交互使用 30 秒局部超时；不放宽产品按钮、状态或自动日志读取语义 | resolved |
| Review-22 | Playwright 折叠卡片合同 | 列表默认只展示登记与进度摘要，操作按钮位于展开区；测试直接查找“查看日志”会把正确折叠行为误判为缺失 | 按真实用户路径先点击实验卡片展开，再显式打开日志；产品折叠/按需日志合同保持不变 | resolved |
| Review-23 | Playwright 日志协议 | 运行态日志由浏览器 `EventSource` 消费，JSON `route.fulfill` 会被正确识别为断流并重连，无法代表终态 tail 合同 | 在“查看日志”按钮已可见后把 mock 状态切为 `completed`，再点击并验证只发起一次 `/logs/tail` 读取；不伪造 SSE 或改变运行态重连逻辑 | resolved |
| Review-24 | 最终 HEAD 与 CI 证据 | 前两次 Playwright 失败分别暴露折叠卡片和日志协议夹具偏差，不能以 Backend-only 结果宣称可合入 | 修正真实用户交互与终态 tail 合同后，最终源码 HEAD 的 AIstock CI run `34101195705` 全绿；PR #4382 为 `MERGEABLE/CLEAN` | passed |
| Review-25 | 用户最新保留原则 | v1.1“全部终态永久保留”与用户要求删除失败、无价值重复实验直接冲突 | 改为执行期全可见、终态 A/B/C/X 分级；失败诊断进入 BUG/Issue，X 不进入长期 QE 历史 | resolved |
| Review-26 | 去重与 partial 安全性 | 只按配置去重会误删不同 seed/股票池/结果，按父级删除会误删成功 sibling | 定义完整 duplicate key、相同配置不同 digest 阻断、canonical survivor 优先级与 child 级清理 | resolved |
| Review-27 | 收益口径与股票池可比性 | `annualized_return` 现有别名会混淆绝对/超额收益，当前 profile 也不能代表历史 loop 股票池 | 指定权威字段来源、禁止年化值相减、缺失显示不可计算；股票池只读 run-scoped binding，legacy 显示历史未记录 | resolved |
| Review-28 | 删除一致性与权限边界 | UI/DB/双节点制品分步失败可能形成半状态，文档合入不能推导生产删除权限 | 设计精确 preview、保护引用、先制品后 DB、可重试 receipt；本 PR cleanup/delete 与 DDL/DML 全部 noop | resolved |
| Review-29 | 过度工程化与主线优先级 | 新规则可能被扩张为通用 GC、事件仓或历史治理工程 | 只复用既有 registry/result/archive/cleanup，禁止 daemon、新数仓、宽泛扫描；v1.2 作为 Batch B 最小增量，不阻断策略实验 | resolved |
| Review-30 | 当前源码可实施性 | 需确认股票池、收益源和精确节点清理不是文档假设 | 当前 `qe_run_registry.py` 已登记 universe/benchmark，`payload_summary.py` 已暴露 absolute/excess-with-cost 来源，`qe_evolution_service.py` 已有节点 API 后清 DB 的精确清理先例；设计只做最小增量 | passed |
| Review-31 | 最终结构与跨文档一致性 | 需确认 19 项 F2 索引、v6.15/F2 v1.2、生产边界和未实现状态不互相矛盾 | F2 validator 19/19、module registry 8 passed、diff-check 通过；v1.1 merged 与 v1.2 design-ready 分开，cleanup/DDL/DML/restart 均为 noop | passed |

### v1.3 本轮增量审核

| 轮次 | 发现 | 修订/结论 |
|---|---|---|
| R32 资格与历史范围 | 仅A入仓/历史一概不处理与用户新要求冲突 | 3.5/3.6改为准确正负A/B/C均可授权入仓、有界历史及有效child保护 |
| R33 指标与复用 | 列表缺全指标/政策真值；prediction相同不等于结果重复；组合不能当单因子绩效 | 4.1/4.5独立训练/交易池、全指标口径与effective政策；3.5精确业务结果去重；QE只提供组合引用 |
| R34 反例与完成状态 | 跨节点清理假原子承诺、HMM全候选豁免、设计pending状态与结构校验不相容 | 逐文件cleanup_incomplete、no-action语义；设计审查与源码/运行态分开，19/19结构通过，不宣称新增业务验证已完成 |

上述为本次文档审核，不重复引用旧版本源码测试为本版实现证据。最终diff复验后交用户确认，不合入、不执行历史写仓/删除。

## 14. Delivery Batch A 实施状态

- 当前源码状态：`BATCH_A_REGISTERED_RUNTIME_READY`。
- 已完成：统一预登记、事务内回读、任务/loop 与 Multi-Alpha parent/group 计划、durable readback、MCP/UI/source/purpose 摘要、只读历史/详情投影、GET 去远端写回、最小 UI 进度与隐藏页零轮询。
- 合入与运行态证据：PR #4382 以 merge commit `c97f4ed471704bd2763af2350b27a8e3de380c87` 合入；用户重启后 backend identity `5bf975e98b7afeaebb27cce73ec250fd268d5090` 包含该提交。health/runtime-identity、实验 summary 列表、双节点 online readback 和前端 Batch A bundle 标识均通过。
- 尚未由 Batch A 推导：Batch B 完整筛选/详情/选择性入仓、Batch C 双节点正式实验和真实 Archive 写入；历史补账、DDL/DML、依赖安装和进程控制仍未执行。

该记录只证明 Batch A source 与最小运行态能力，不表示整个 Feature 已完成。Batch B/C 仍须按同一 19 项验收矩阵继续。

## 15. Delivery Batch B 实施状态

- v1.1 源码状态：已通过 PR #4397 合入，merge commit `7ccf06a9e`；本设计未重新核验其当前运行态身份，因此只确认 source merged，不推导 runtime ready。
- v1.2 增量状态：`VALUE_RETENTION_DELTA_DESIGN_READY`；失败/精确重复 X 清理、A/B/C 分类、trajectory 实际股票池与四类收益字段尚未实现，不能把 v1.1 的“全部正式终态保留”能力宣称为满足最新要求。
- 已实现：日期、来源、实验类型、Alpha 模式、用途、canonical 状态、节点、模型、因子、数据 release、股票池、分钟执行算法、Archive 状态和业务文本的服务端筛选；父级稳定分页；UI 不再默认全量拉取；MCP 暴露相同的人类可读筛选。
- 已实现：列表与详情展示登记、节点、release/cutoff、股票池、seed、label、分钟执行、时间线、失败原因、artifact retention 和 Archive 推荐/状态；日志只在显式点击后读取。
- v1.1 已实现：正式登记 experiment/task/loop 的 workspace 清理保留控制记录和指标，并写入 `_qe_artifact_retention.status=cleaned`；未登记 legacy 行继续保持既有删除兼容性。该行为在 v1.2 中仍适用于 A/B/C，但不能用于 X 的一致删除。
- 本地证据：聚焦 Backend/MCP/cleanup/Archive 状态合同 58 passed；`qe_read_backend` 294 passed / 1 skipped；`qe_data_contract_backend` 46 passed；F2 validator 19/19；L0 blocking=0；changed-files ownership 15/15；CI classifier 本地复验 `targeted_ci_required`、`unmapped_code_files=[]`、`unexecuted_test_files=[]`；`test_noxfile_validation_env.py` 17 passed；validation catalog/module registry 均通过；Ruff、py_compile、TypeScript syntax transpile 和 diff-check 通过。Archive 状态筛选在释放历史查询连接后再调用现有 Archive service，测试确认单次请求不存在双连接重叠占用。
- 验证委托：worktree 未安装依赖、未启动本地前后端；TypeScript 类型检查、Lint、精确 mocked Playwright、Archive 全回归和跨模块业务流交由最终 PR CI。Batch C 的真实 WSL/remote、用户重启恢复、UI 与授权 MCP Archive 写入仍不在本批源码结论内。
- 生产边界：DDL/DML=noop，dependency install=noop，Archive write=noop，experiment submission=noop，dataset write=0，backend/worker/frontend process control=false。

## 16. v1.3 增量实施顺序

1. 先修准确投影和科创50Top20消费合同：收益/风险/IR/费用/实际政策与训练/选股池同源回读，GET只读；不让不准确摘要参与自动排名。必要predicate和UI列组复用现有代码，不扩平台。
2. 落实所有准确A/B/C正负样本的Archive资格、组合证据与单因子边界；有界历史只读盘点和父预测复用清单可并行，不先等清理。
3. X精确preview/apply按4.8的部分失败与保护引用处理；生产DB修改先既有DEV验证再明确授权，无需预设新增schema。
4. 本Feature仍3批次：A既有登记/状态，B本版最小增量，C真实验收；多轮代码审核/合同测试与最终CI按实际changed files，source merge、用户重启、运行态、Archive写入及删除分开。
5. 本版文档未执行源码修复、Archive/清理、实验或进程控制。旧14/15节只保留各批次历史证据，不证明v1.3已经完成。

### v1.3 新增验收反例

- 纯star50/别名/clone/MCP/重放非20拒绝、默认20、大并集不误判；历史Top50不改写；停牌滞留不虚构卖出。
- 相同prediction不同HMM/黑名单/交易池不是重复；共享预测/parent有效child不得删；同配置不同权威结果保留。
- A/B/C均可授权入Archive，负收益不误删、不需先升A；无证据缺失字段不得手动绕过。
- HMM缺状态显示unknown、未触发不等于off；当前名单重构不得当历史实际名单；训练池/交易池独立可见。
- 同净值MDD/Calmar、IR非Sharpe、费用不重扣、全部已产出指标在trajectory正常列组可查；七八月分段与h20成熟样本分开。
- 删除部分失败UI逐文件状态准确、重试幂等、DB未提前删除、跨节点及survivor引用均可回读；保留试验计数不制造选择偏差。
- 因子参与引用不修改因子官方评分，组合收益不得写成单因子表现。
