# QE 实验数仓与资产生命周期闭环 F2 详细设计

> Feature ID：`qe_experiment_asset_warehouse_lifecycle_v1`
> Feature tier：F2
> 设计版本：v1.0
> 日期：2026-09-22
> 状态：`DESIGN_VERIFIED_IMPLEMENTATION_PENDING`
> 适用范围：QE 正式实验结果、QE Archive、受控二进制资产、StrategyPackage alpha core 引用、QE workspace 生命周期
> 上位实验登记设计：`docs/architecture/qe_unified_experiment_registry_selective_warehouse_f2_design_20260907.md`
> 既有资产治理设计：`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`

本文是上述两份设计在“什么进入数仓、哪些二进制资产长期保存、StrategyPackage 如何引用、workspace 何时清理”四个问题上的最新权威补充。冲突时以本文为准；实验登记、执行、模型、因子、分钟执行、SOTA 人工晋级和生产交易边界仍由各自原设计负责。

## 1. Background / 背景

QE 已有控制面登记、QE Archive、StrategyPackage 资产索引和 workspace cleanup 能力，但其生命周期合同尚未闭环：

1. 旧规则把“技术有效的研究事实进入数仓”与“人工选择优秀结果”混在一起，导致有效负结果可能遗漏，也让数仓无法完整支持后续统计分析。
2. 失败、无有效结果、validation 噪声和精确重复曾被描述为 X 档后再删除数仓记录；最新业务决定是它们从一开始就不得进入 QE Archive。
3. 大文件仍可能依赖可清理 workspace；进入策略包的资产缺少“逻辑双重引用、物理单份存储”的统一合同。
4. 当前只有本机 X 盘约 2.6 TB 可用空间，没有第二故障域。此时引入对象存储或在同盘复制两份，只增加复杂度和占用，并不形成真实备份。
5. workspace 确认不作长期资产库，但清理必须在数仓事实和必要资产均已持久化并回读后进行，不能制造 UI 有记录而文件丢失或策略包依赖失效。

本设计用现有 QE Archive、现有 `strategy_pkg.package_asset`/资产接口和精确 cleanup receipt 完成闭环，不建立新数仓、对象存储集群、通用 GC 平台或新常驻服务。

## 2. Scope / 范围

### 2.1 目标

1. 所有技术有效、身份完整、结果可评价、非精确重复的正式实验自动写入 QE Archive 的 Level-0 结构化研究事实；收益正负不影响准入。
2. A/B/C 仅决定结构化保存深度和二进制资产 retention，不再决定是否有资格进入数仓。
3. X 类——失败、无有效结果、validation/fixture 噪声和已证明的精确重复——禁止进入 QE Archive。
4. 在 X 盘建立单一内容寻址资产根；相同 SHA256 只保存一份物理 blob，Archive 与 StrategyPackage 使用独立逻辑引用。
5. 对进入 StrategyPackage 的 alpha core 资产提供长期保护，但不在同一块 X 盘制造“第二副本”假象。
6. workspace 保持短期、可清理；通过精确 manifest、引用检查、活动进程检查和分步回执保证控制面、数仓、资产和文件状态一致。
7. UI/API 分别显示计算终态、数仓状态、资产发布状态和 workspace 清理状态，禁止用单一 `completed` 掩盖部分完成。
8. 保留多重检验所需的实验协议、候选数和失败聚合统计，但不把 X 伪装成可评价实验样本。

### 2.2 适用对象

- registered single-alpha；
- custom/strategy/auto evolution 的可评价 loop；
- Multi-Alpha 可评价 parent/child/group；
- prediction-replay、股票池/HMM/黑名单/执行政策 matched comparison；
- 被人工晋级为 StrategyPackage 的 alpha core 及其可复现资产。

父任务为 `partial` 或 `failed` 时，逐最小可评价 child/loop 分类；成功且有效的 child 仍按 A/B/C 入仓，父容器只保留必要 lineage，不把整个父任务归为 X。

## 3. Non-Goals / 非目标

- 不引入 S3/MinIO/Ceph、Kubernetes、独立对象存储服务或第二资产数据库。
- 不把同一物理 X 盘上的第二份复制称为备份；没有第二故障域前不承诺双副本容灾。
- 不永久保存完整 workspace、训练缓存、依赖环境、临时日志或重复预测文件。
- 不改变 QE 模型、因子、label、seed、数据切分、股票池、HMM、行业黑名单、TWAP 或费用语义。
- 不把组合回测收益、IC 或风险指标写成单个因子的官方绩效。
- 不建立新的人工审批、收益阈值或研究准入门槛。自动持久化和资产完整性检查是程序一致性，不是研究审批。
- 不扫描全部历史目录、不猜测 DB 与文件关系、不自动物化已经缺失的历史资产。
- 本设计 PR 不执行 DDL/DML、历史补录、文件删除、依赖安装、实验提交或进程控制。

## 4. Architecture / 架构

### 4.1 五层职责

| 层 | 权威职责 | 长期性 | 禁止职责 |
|---|---|---|---|
| QE 控制面 | 在途状态、重试、节点、failure reason、X 聚合统计、artifact lifecycle | 运行期及必要诊断期 | 作为研究事实数仓或大文件仓库 |
| QE Archive | 技术有效且唯一的 A/B/C 结构化研究事实、指标、配置、曲线/成交摘要、lineage | 长期 | 接收 X、保存完整 workspace、保存不可解释的伪指标 |
| X 盘 CAS | 由 SHA256 标识的必要模型/预测/曲线/成交明细/代码快照等 blob | 按 retention/refcount | 保存临时缓存、目录快照或不受控重复副本 |
| StrategyPackage | alpha core 的 manifest、逻辑资产引用和保护状态 | 晋级期间长期保护 | 直接依赖 workspace；复制同一 blob 制造假双副本 |
| QE workspace | 计算、诊断、短期复核与资产发布源 | 短期 | 充当永久数仓、策略包资产库或知识库 |

### 4.2 终态处理流

```text
计算进入候选终态
  -> 校验 canonical identity / dataset / config / result completeness
  -> 逐最小可评价对象判定：A/B/C 或 X
     -> A/B/C：幂等写 QE Archive Level-0
                -> 按档位发布必要 blob 到 X 盘 CAS
                -> 写逻辑引用与 readback receipt
                -> 满足宽限期后进入 workspace cleanup
     -> X：绝不调用 QE Archive writer
           -> 控制面保留失败/噪声/duplicate reason 与聚合计数
           -> 必要时绑定 BUG/Issue/日志证据
           -> 满足短宽限期后进入 workspace cleanup
```

计算终态、Archive 持久化、CAS 资产发布、StrategyPackage 保护和 workspace 清理是五个独立状态。任一步失败都必须显式显示 typed reason，并可从该步幂等重试；不得把前一步成功推导为整个闭环成功。

### 4.3 物理拓扑

第一阶段仅使用本机 X 盘，例如：

```text
X:\AIstock_state\qe_asset_store\
  blobs\sha256\ab\cd\<full-sha256>
  staging\<publication-id>\
  receipts\<yyyy-mm>\<publication-id>.json
```

- `blobs` 是唯一受控物理资产根；正式 blob 文件名由完整 SHA256 决定。
- `staging` 只允许同盘临时写入，完成 size/hash 校验后原子 rename；失败时不生成可引用 blob。
- `receipts` 保存发布/校验/清理的轻量回执，不包含凭据。
- 资产根只通过现有全局 `AISTOCK_PACKAGE_ASSET_STORE_ROOT`（或实现时确认的同一权威配置项）解析；UI、MCP、WSL、node1 和业务代码不得各自硬编码目录。上述 X 盘路径只是部署目标示例，不是散落到源码的常量。
- WSL 与 node1 不直接挂载或修改该根。Backend/资产服务通过既有节点 API 拉取候选文件，在 Windows 控制端完成 hash、原子发布和登记。
- 现有 `AISTOCK_PACKAGE_ASSET_STORE_ROOT` 后续指向该根；不在 repo、worktree、dataset candidate 内建资产库。

## 5. Contracts / 合同

### 5.1 A/B/C/X 分类合同

| 级别 | 准入条件 | Archive | 二进制资产 |
|---|---|---|---|
| A | 正式锚点、冠军/Pareto、关键 matched baseline/variant、策略包候选；技术有效且唯一 | 自动完整结构化入仓 | 永久保存可复现/上线必需的权重、预处理、schema、代码、预测/交易证据 |
| B | 有效研究结果、代表性负对照、跨 seed/vintage/pool 证据；技术有效且唯一 | 自动完整结构化入仓 | 权重及深层明细至少 180 天；被下游引用即转 protected，结构化事实永久 |
| C | 增量有限但仍可评价、可用于统计或反例；技术有效且唯一 | 自动最小完整结构化入仓 | 权重/大明细默认 180 天后可清理；配置、代码/schema digest、指标和 lineage 永久 |
| X | failed/cancelled 且无有效结果、validation/fixture 噪声、无结果运行、已证明精确重复 | **禁止入仓** | 不作长期发布；仅保留必要诊断至清理宽限期 |

技术有效性与收益无关。准确的负收益、低 IC、策略退化或否定假设的结果仍是 A/B/C 研究事实；它们不能因表现差被归为 X。

分类必须自动收敛且不形成新的人工门禁：技术校验通过且非重复的结果至少自动归为 C 并立即写 Level-0；显式锚点/matched baseline/策略包引用/预注册代表性角色可按确定性规则升为 B/A。`classification_pending` 只表示身份、结果或引用校验尚未闭合，此时对象保持可见且禁止清理；它不是要求用户逐条审批。档位调整保留审计记录，降档不得追溯删除仍处于 retention 或受引用保护的资产。

X 的失败诊断属于控制面或 BUG/Issue 证据，不属于 QE Archive 实验事实。精确重复仅保存 canonical survivor 的 Archive/资产；重复对象只保留 survivor identity、业务 digest、duplicate reason 和聚合计数，不能形成第二条 Archive 样本。

### 5.2 精确重复合同

只有以下业务身份全部相同，且权威结果内容 digest 相同，才可标记 exact duplicate：

- dataset release、各组件 manifest、源码和配置 schema；
- 模型代码/处理器/超参/seed、特征、label、split；
- 训练、推理、交易股票池及 PIT 身份；
- 窗口、benchmark、TopK、费用、分钟执行；
- HMM/黑名单/其他政策及有效区间；
- prediction、组合结果和核心 artifact 的规范化业务 digest。

相同 prediction 但不同交易池/政策/执行参数是正式 matched comparison，不是重复。相同配置但结果 digest 不同必须保留并投递确定性诊断，禁止自动删除。时间戳、workspace path、节点路径等非业务字段不得制造伪差异。

### 5.3 Archive 自动写入合同

1. Archive projector 仅接受已完成技术校验且分类为 A/B/C 的最小可评价对象。
2. 写入以 canonical experiment/loop/child identity 加业务 result digest 幂等；重试不产生新样本。
3. Level-0 至少包括：来源/lineage、完整数据身份、模型/因子/seed/label/split、训练/推理/交易池、政策、成本/执行、完整已产出指标、窗口、artifact manifest/digest 和 validity scope。
4. A/B 可在 Level-0 上增加完整曲线、归因、成交/持仓摘要；C 仍必须包含可解释的最小身份和指标，不能退化成只有收益率的一行。
5. 组合表现只登记为组合/实验结果；因子库只能引用“该因子参与某组合”的研究证据，不能把组合收益或组合 IC 覆盖为单因子官方评分。
6. 自动写入失败保持 `warehouse_pending`/`warehouse_failed` 和稳定 reason code；不得降级为 X，也不得在持久化前清理 workspace。
7. 人工操作只用于 SOTA/StrategyPackage 晋级、深层资产延长或有界历史修复，不再作为新 A/B/C Level-0 入仓前置条件。

### 5.4 X 禁止入仓合同

- 分类为 X 后，任何 QE Archive preview/execute/projector 都必须返回 `not_eligible`，且不得创建 Archive run/loop/result。
- validation/smoke 若意外产出完整指标，仍按声明 purpose 判定为 X；不得作为正式研究样本。
- 父任务失败但有有效 child 时，只对无结果 child/attempt 使用 X；有效 child 正常入仓。
- 对历史上误入 Archive 的 X 不在本功能上线时静默删除。它们进入单独的 legacy anomaly 清单，未来按精确 identity、DEV 验证、明确生产授权和 readback 修复。
- 多重检验所需 trial count、协议、失败类型聚合保留在研究批次/控制面，不要求保留每条 X 为 Archive 样本。

### 5.5 CAS 发布与引用合同

每个受控 asset reference 至少包含：

```text
sha256
size_bytes
media_type
logical_role
producer_identity
source_receipt
retention_class
protected_by
created_at
verified_at
availability
```

发布顺序：节点精确文件定位 -> 流式 hash/size -> X 盘 staging -> 二次 hash/size -> 原子 rename -> DB/manifest 引用 -> readback。目标 SHA 已存在时必须核对 size/hash 后复用，不重写。目标存在但内容不一致属于 collision/corruption，loud fail。

CAS 文件删除只有在所有逻辑引用均释放、retention 到期、无活动读者/发布操作、精确 manifest 命中时允许。Archive 和 StrategyPackage 引用是独立逻辑所有者；任何一个仍保护该 SHA，物理 blob 都不得删除。

### 5.6 StrategyPackage 双重保护语义

进入 StrategyPackage 的模型权重、预处理器、feature schema、因子代码/顺序、冻结配置和复现清单必须：

1. 不再依赖 workspace 路径；
2. 在 StrategyPackage manifest 和 QE Archive artifact manifest 中各有独立不可变引用；
3. 两个引用可以指向同一个 CAS SHA 和同一物理文件；
4. `retention_class=protected`，直到 StrategyPackage 正式退役且所有下游引用解除；
5. runtime 节点缓存仅是可重建缓存，不计作备份或权威副本。

当前硬件条件下不制造第二物理副本。未来获得另一块独立磁盘、NAS 或远端存储后，再在不改变逻辑引用的前提下增加跨故障域复制策略。

### 5.7 Workspace cleanup 合同

| 对象 | 最短宽限期 | 清理前置条件 |
|---|---:|---|
| A/B/C 成功 workspace | 7 天 | Archive readback 完成；档位所需 CAS 资产发布并校验；下游不再直接引用 workspace；无活动进程 |
| X 失败/无结果 | 3–7 天 | failure reason 或 BUG/Issue 证据已保留；无活动进程；无有效 sibling/shared asset 引用 |
| X validation/精确重复 | 24–72 小时 | purpose/duplicate survivor 与 digest 已确认；聚合计数已保留；无活动/共享引用 |

清理必须满足：

- 输入是精确 experiment/task/loop/attempt identity 和已登记 artifact manifest；
- 逐文件验证路径边界、普通文件类型、无 symlink/junction、无 tracked file；
- 检查活动进程、共享 prediction/权重、Archive、StrategyPackage、冠军和下游任务引用；
- 不允许通配符、递归目录发现、`git clean` 或 DB/文件双向猜测；
- 先删除节点/workspace 文件并取得逐项 receipt，最后更新控制面 artifact 状态；
- 部分失败保留 `cleanup_incomplete` 和每项 `available/deleted/unknown`，安全重试不得重复删除共享资产；
- X 原本不在 Archive，因此正常 X cleanup 不执行 Archive delete。只有单独登记的 legacy anomaly 修复可触及历史 Archive 行。

### 5.8 UI/API 状态合同

列表/详情至少分开显示：

- `compute_status`；
- `warehouse_status`：`not_eligible | pending | persisted | failed`；
- `asset_status`：`not_required | pending | published | partial | failed`；
- `workspace_status`：`active | grace_period | cleanup_pending | cleaned | cleanup_incomplete`；
- `value_class`：`A | B | C | X | classification_pending`；
- `duplicate_of`（仅精确重复）；
- `retention_summary` 和 protected owner 数量。

UI 不以 X 为可评价样本，不把 X 纳入收益排行、均值、DSR/PBO 绩效矩阵或因子表现。研究协议层仍可展示失败/重复计数，避免幸存者偏差。GET、页面打开或健康检查不得触发 Archive/CAS/cleanup 写入。

### 5.9 容量、完整性与运行边界

- 第一阶段以单一全局配置声明 X 盘 CAS 逻辑预算（初始建议约 1 TB）；70%/85% 是可配置的 warning/critical 默认值，仅告警而不是研究审批门禁，也不得散落硬编码到不同入口。
- 空间不足、写入失败、hash 不一致或引用不完整必须 loud fail；不得静默回退 workspace、repo 或旧数据目录。
- 当前不以对象存储解决单盘风险。资产完整性依靠 SHA256、原子发布、定期只读校验和引用 readback；真正容灾等待第二故障域。
- 不新增高频心跳或每次 UI 查询写事件。容量/完整性巡检复用现有有界任务，正常无变化不写事件。
- 优先复用现有 schema。只有实现证明既有字段无法表达合同，才提出 migration，并严格分离 DEV 验证、生产授权、apply 与 readback。

## 6. Design Acceptance Index / 设计验收索引

| 编号 | 设计要求 |
|---|---|
| F-001 | 所有技术有效、身份完整、可评价、非精确重复的 A/B/C 正负结果自动进入同一 QE Archive |
| F-002 | A/B/C 只决定保存深度和二进制 retention，不作为数仓准入收益门槛 |
| F-003 | failed、无结果、validation 噪声和精确重复 X 从入口即禁止进入 QE Archive |
| F-004 | partial/failed parent 逐 child 分类，有效 child 不被父状态吞掉 |
| F-005 | 精确重复使用完整业务身份与结果 digest，相同配置不同结果和不同政策 matched comparison 不误删 |
| F-006 | 组合表现只属于组合实验，不覆盖单因子官方绩效 |
| F-007 | X 盘 CAS 使用 SHA256、同盘 staging 原子发布、碰撞 fail closed 和物理去重 |
| F-008 | Archive 与 StrategyPackage 各自持有逻辑引用，但同一 SHA 只保存一个物理 blob |
| F-009 | StrategyPackage alpha core 不依赖 workspace，晋级资产长期 protected |
| F-010 | A/B/C 与 X 分级宽限期和精确 workspace cleanup 条件完整、可重试且不猜测 |
| F-011 | compute、warehouse、asset、package protection 和 cleanup 状态分别可读，不伪造整体完成 |
| F-012 | 多重检验保留协议和失败聚合计数，但 X 不进入可评价样本矩阵 |
| F-013 | 单盘条件下不引入对象存储或同盘假双副本，未来扩容不改变逻辑引用 |
| F-014 | 不新增人工研究门禁、通用平台、第二数仓、daemon 或高频状态事件 |
| F-015 | DDL/DML、生产清理、运行时激活和用户后端重启与文档/source merge 分开 |

## 7. Implementation Plan / 实施计划

保持三个交付批次，不拆成大量串行项目。每批只在实际 ownership 需要时技术性拆 PR，不形成新的业务阶段或审批。

### Batch A：自动 Archive 投影与 X 排除

1. 在现有 terminal finalization/Archive assembler 前增加单一 eligibility/classification 入口。
2. A/B/C 自动幂等写 Level-0；负结果与有效 child 覆盖。
3. X 在 projector 边界返回 `not_eligible`，禁止创建 Archive 行。
4. 暴露分离的 warehouse/asset/workspace 状态；自动重试持久化失败，不清理未持久化对象。
5. 增加 exact duplicate survivor、partial parent 和组合/因子归因边界测试。

结束标识：`QE_ARCHIVE_VALID_UNIQUE_AUTO_PERSIST_READY`。

### Batch B：X 盘 CAS 与 StrategyPackage 单 blob 引用

1. 将现有 package asset store root 配置到 repo 外 X 盘状态根。
2. 实现/收口本地 CAS 的 staging、hash、原子发布、复用、readback 和 typed failure。
3. Archive artifact manifest 与 StrategyPackage manifest 分别引用同一 SHA；实现保护 owner/refcount 语义。
4. 按 A/B/C retention 发布必要资产，不复制完整 workspace。
5. 完成 WSL/node1 精确文件拉取 smoke，不让节点直接写 X 盘 CAS。

结束标识：`QE_CAS_PACKAGE_REFERENCE_READY`。

### Batch C：精确清理、对账与 UI 闭环

1. 按 5.7 实现宽限期、保护引用、活动进程和路径类型检查。
2. A/B/C cleanup 以 Archive/CAS readback 为前置；X cleanup 不碰 Archive。
3. 提供 cleanup preview/receipt、部分失败重试和 legacy anomaly 独立清单。
4. UI/API 分离展示五类状态、X exclusion、duplicate survivor 和 retention。
5. 运行只读 reconciliation，确认 DB 引用、CAS 和 workspace 状态一致；不做目录猜测式修复。

结束标识：`QE_ASSET_LIFECYCLE_RUNTIME_VERIFIED`。

三批完成状态只表示本功能技术闭环，不是新 Alpha 研究、StrategyPackage 晋级或模拟盘准入门槛。

## 8. Verification Plan / 验证方案

### 8.1 合同测试

- 正收益、负收益、低 IC、决定性负对照分别自动入仓，Archive identity/digest 幂等。
- failed/no-result/cancelled、validation、精确重复分别证明 Archive 写入数为 0。
- partial parent 的有效 child 入仓，无结果 sibling 为 X；父 lineage 仍可读。
- 相同 prediction 不同 HMM/黑名单/股票池/TWAP 作为独立正式结果；相同配置不同 digest 不去重。
- 组合结果不能写入单因子官方评分字段。
- CAS 新发布、同 SHA 复用、hash 漂移、目标碰撞、磁盘不足、staging 中断均有确定结果和 stable reason code。
- Archive + StrategyPackage 两个 owner 指向一个物理 blob；释放一个 owner 不删除 blob，最后 owner 释放且 retention 到期才可删。
- workspace cleanup 覆盖活动进程、shared asset、protected package、symlink/junction、tracked/unknown 文件和部分失败重试。
- X cleanup 不发 Archive DELETE；legacy anomaly 无单独授权保持只读。
- GET/UI/健康检查保持只读，正常巡检无事件写入。

### 8.2 真实验收

1. WSL 和 node1 各选一个技术有效正式结果，验证自动 Archive、CAS 发布、readback 和宽限期状态。
2. 选一个有效负结果，确认不会因收益差被归 X。
3. 选一个 validation 和一个 exact duplicate，确认 Archive 为 `not_eligible`、survivor 不受影响。
4. 从同一个 CAS blob 创建 Archive 与 StrategyPackage 两个引用，清理 workspace 后完成 StrategyPackage 资产加载 smoke。
5. 模拟发布中断和 cleanup 部分失败，确认状态真实、可重试、无共享资产误删。
6. 只读容量/引用审计确认 X 盘实际物理 blob 数与唯一 SHA 数一致。

### 8.3 本设计文档门禁

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_experiment_asset_warehouse_lifecycle_f2_detailed_design_20260922.md --tier F2`
- `git diff --check`
- 逐项 DESIGN-COMPLIANCE-001 审核和跨文档冲突扫描。

## 9. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | planned Batch A classifier/projector | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` positive/negative auto-persist cases | DESIGN_VERIFIED | - |
| F-002 | planned Archive retention projection | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` A/B/C depth matrix | DESIGN_VERIFIED | - |
| F-003 | planned X eligibility rejection | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` four X classes assert zero writes | DESIGN_VERIFIED | - |
| F-004 | planned child-level finalization | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` partial-parent mixed-child cases | DESIGN_VERIFIED | - |
| F-005 | planned canonical duplicate digest | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` matched-policy/nondeterminism cases | DESIGN_VERIFIED | - |
| F-006 | existing factor/QE boundary plus planned assertions | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` combination-to-factor negative cases | DESIGN_VERIFIED | - |
| F-007 | planned local CAS implementation/config | `backend/tests/strategy_pkg/test_qe_local_cas.py` atomic publish/dedupe/collision cases | DESIGN_VERIFIED | - |
| F-008 | existing package asset refs plus planned owner semantics | `backend/tests/strategy_pkg/test_qe_local_cas.py` two refs/one blob lifecycle | DESIGN_VERIFIED | - |
| F-009 | planned package freeze readback | `backend/tests/strategy_pkg/test_qe_local_cas.py` workspace-removed package load smoke | DESIGN_VERIFIED | - |
| F-010 | existing precise cleanup patterns plus planned lifecycle service | `backend/tests/unified_engine/test_qe_asset_lifecycle_cleanup.py` protection/path/process/partial-failure cases | DESIGN_VERIFIED | - |
| F-011 | planned API/UI status projection | `frontend/tests/quantevolver/qe_asset_lifecycle.spec.ts` separated-state readback | DESIGN_VERIFIED | - |
| F-012 | planned research protocol summary | `backend/tests/qe_archive/test_qe_asset_lifecycle.py` X metrics exclusion/protocol count case | DESIGN_VERIFIED | - |
| F-013 | local X-disk CAS topology in §4.3/5.6 | `backend/tests/strategy_pkg/test_qe_local_cas.py` unique-SHA physical count audit | DESIGN_VERIFIED | - |
| F-014 | reuse existing QE Archive/package/cleanup capabilities | validation-receipt: qe-asset-lifecycle-architecture-side-effect-review | DESIGN_VERIFIED | - |
| F-015 | Production Gates in §12 | validation-receipt: qe-asset-lifecycle-delivery-state-separation | DESIGN_VERIFIED | - |

矩阵的 `DESIGN_VERIFIED` 只表示设计要求、实施引用和验收证据闭合，不表示源码、运行态、历史数据修订或清理已经完成。

## 10. Rollout / Rollback / 发布与回滚

### 10.1 Rollout

1. 本次先合入 docs-only 设计和两份旧设计的权威指针，不产生运行态变化。
2. Batch A/B/C 分别从实施时最新 `origin/main` 开发、审核、测试和合入；每批按实际 changed files 计算 runtime contract。
3. 如确需 migration，先在既有 DEV 数据库验证，再单独报告 production target、授权、apply 和 readback；不得由 source merge 自动推导。
4. Backend 重启始终由用户执行；WSL/node1 API 或资产根激活按各自运行合同单独处理。
5. 不以全历史补录或清理作为新 QE 实验继续运行的前置条件。

### 10.2 Rollback

- Batch A 回滚：暂停新的自动 projector，保留已经写入的有效 Archive 行，不删除研究事实；恢复只读查询，修复后幂等续投。
- Batch B 回滚：停止新资产发布，保留已验证 CAS blob 和引用；StrategyPackage 不回退 workspace 路径。
- Batch C 回滚：停止新的 cleanup apply，继续允许 preview/readback 和 `cleanup_incomplete` 修复；不得恢复通配符删除。
- 任何回滚均不得把 X 写入 Archive、删除有效负结果、恢复同盘双复制或静默回落旧资产根。

## 11. Risks / 风险

| 风险 | 处理 | 禁止做法 |
|---|---|---|
| 自动入仓造成容量增长 | 全部结构化事实入仓，但大资产按 A/B/C retention；CAS 去重 | 自动复制完整 workspace |
| X 排除造成幸存者偏差 | 协议和失败/重复聚合计数保留，绩效矩阵只含可评价结果 | 把 failed 当零收益样本或删除有效负结果 |
| 分类错误误删 | `classification_pending` 不清理；严格 duplicate digest；引用不确定 fail closed | 仅按收益、配置或文件名分类 |
| 单盘故障 | 明确无真实备份；hash/原子写/巡检；未来增加第二故障域 | 同盘复制并宣称容灾 |
| StrategyPackage 资产随 workspace 丢失 | package freeze 前先 CAS 发布和双引用 readback | package 直接引用 workspace |
| DB 与文件半状态 | 分离状态、逐项 receipt、cleanup_incomplete 可重试 | 假跨系统事务或先删 DB |
| 过度工程化 | 复用 Archive/package asset/cleanup；三批交付 | 新对象存储、消息总线、通用 GC/daemon |
| 研究主线被治理阻断 | 自动化持久化；历史修订并行；无人工 Archive gate | 等全历史清理完才允许实验 |

## 12. Production Gates / 生产边界

| 状态项 | 本设计 PR | 后续实现/运行态 |
|---|---|---|
| source merge | 已获本轮文档合入授权 | 各源码批次按用户后续任务授权 |
| DEV DDL/DML | noop | 仅实现证明需要时，先 DEV 验证 |
| production DDL/DML | noop | 必须独立明确授权和 readback |
| Archive writes/backfill | noop | 新结果由未来 Batch A 自动；历史修订独立授权 |
| CAS root activation | noop | Batch B 独立配置/readback |
| cleanup/delete | noop | Batch C 精确 preview、授权和 receipt |
| dependency install | noop | 不预期新增依赖 |
| experiment submission | noop | 与本设计合入无关 |
| Backend restart | false | 始终由用户执行 |
| dataset write/activation | 0 / noop | 不属于本功能 |

## 13. DESIGN-COMPLIANCE-001 / 四项审核

| 检查项 | 结论 | 直接证据 |
|---|---|---|
| 禁止简化、子集、POC、占位或 mock-only 冒充完成 | PASS（设计） | 覆盖 Archive 准入、X 排除、CAS、StrategyPackage 引用、cleanup、UI 状态和真实双节点验收；明确源码仍 pending |
| 禁止静默错误或伪成功 | PASS（设计） | hash/identity/引用不确定均 fail closed；五类状态分离；部分失败可读且不伪装完成 |
| 禁止未经确认改变业务逻辑 | PASS（设计） | 仅落实用户确认的 A/B/C 自动入仓、X 禁止入仓与 workspace 非长期资产；不改模型/因子/执行/晋级语义 |
| 禁止私增门禁、审批或人工确认 | PASS（设计） | 删除新实验手工入仓前置；仅保留技术一致性和既有 SOTA/StrategyPackage 人工晋级，不设收益/研究准入门槛 |

## 14. 多轮审核记录

| 轮次 | 审核重点 | 发现 | 修订 | 状态 |
|---|---|---|---|---|
| Review-1 | 跨文档与业务语义 | 旧设计仍要求 A/B/C 人工选择后入仓，并把 X 描述为删除数仓记录 | 本文确立自动 Level-0 与 X 零 Archive；同步旧设计权威指针和关键条款 | resolved |
| Review-2 | 数据安全与失败恢复 | 若把 Archive、CAS、cleanup 合成单一 completed，会在部分失败时丢失真实状态 | 拆分五类状态、幂等重试、先资产后清理、X 正常路径无 Archive DELETE | resolved |
| Review-3 | 过度工程化与硬件现实 | 单 X 盘上部署对象存储或复制两份不能形成容灾 | 采用本地 CAS 单物理 blob、逻辑双引用；对象存储和第二副本延期至第二故障域可用 | resolved |
| Review-4 | 自动收敛、可实施证据与最终结构 | A/B/C 若仍需人工逐条分类会形成新门禁；初版矩阵证据名称不够具体 | 有效唯一结果默认至少C并自动入仓；资产根单点配置；新设计15/15、上位设计19/19 F2校验通过，diff/scope检查通过 | passed |

## 15. 结论

本设计完成的闭环是：**技术有效且唯一的正式实验事实自动进入数仓；A/B/C 决定深度，不决定准入；X 永不进入数仓；必要大资产进入 X 盘单一 CAS；策略包与数仓独立引用同一 blob；workspace 在持久化与保护引用闭合后精确清理。**

它解决资产丢失、无效实验污染、重复占盘和 workspace 永久膨胀，同时没有引入当前硬件无法兑现的对象存储/双副本承诺，也不把资产治理变成新的研究审批门槛。
