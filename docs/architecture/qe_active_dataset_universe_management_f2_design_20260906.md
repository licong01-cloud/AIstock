# QE 活动数据集与 PIT 多股票池统一管理 F2 详细设计

> Feature ID：`qe_active_dataset_universe_management_v1`  
> Feature tier：F2  
> 日期：2026-09-06  
> 状态：详细设计已合入；源码实现完成并处于合入前验证，活动 profile、运行态和正式实验均未激活
> 当前实现基线：设计 merge `f05ee69d2c29867703d345c4c9d26cf5700a03be`

## 1. Background / 背景与当前事实

QE 当前存在两个应当一次解决、不能继续分别打补丁的问题：

1. 新实验的默认数据截止时间分散在后端、模板校验器、前端和多 Alpha 创建器中。当前主线仍存在
   `2026-06-29`、`2026-06-30`、`2026-04-27`、`2026-04-28` 等历史默认值。即使 2026-08-31
   direct-v2 数据集已经准备完成，UI、MCP 或不同创建入口仍可能生成较早的回测窗口。
2. 核心指数成分 PIT authority 和小型 instruments sidecar 已经落地，但 QE direct-v2 合同、配置组装器和
   frozen risk-policy builder 仍把选股池固定为 `stock_universe`，不能规范选择沪深300、中证500、中证1000、
   科创50、科创100或多指数并集。

### 1.1 已落地的数据能力

生产只读回执表明 `market.core_index_membership_pit` 已覆盖以下五个 P0 股票池，共 4,866 条原始成员区间：

| pool_id | 指数 | 指数代码 | 2026-08-31 原始成分数 | 与 canonical equity PIT 相交后的可选数 |
|---|---|---|---:|---:|
| `csi300` | 沪深300 | `000300.SH` | 300 | 299 |
| `csi500` | 中证500 | `000905.SH` | 500 | 496 |
| `csi1000` | 中证1000 | `000852.SH` | 1,000 | 990 |
| `star50` | 科创50 | `000688.SH` | 50 | 48 |
| `star100` | 科创100 | `000698.SH` | 100 | 100 |

数据准备侧已经生成五个 primitive sidecar 和一个五指数并集 sidecar；集合与共享 resolver 一致、重复区间为 0、
非股票代码为 0，且没有修改来源 candidate。当前缺口不是 authority，也不是重新导出日线、分钟线或因子大组件，
而是：

- sidecar 尚未注册到 QE 活动数据集 profile；
- QE 尚不能把控制端已钉 hash 的语义化股票池选择解析为 node-specific、hash-pinned run binding，并随既有 workspace
  payload 发送到执行节点；无需为此修改 candidate 或维护第二份节点 sidecar 根。

### 1.2 当前 QE 代码约束

- `qe_dataset_contract.py` 的 direct-v2 binding 只允许 `selection_pins.stock_pool=stock_universe`；
- `qe_validate_direct_v2_dataset.py` 只校验 `stock_universe.txt`；
- `config_composer.py` 会拒绝 direct-v2 binding 之外的股票池；
- `qe_build_frozen_risk_policy.py` 只接受 `all.txt` 或 `stock_universe.txt`；
- `stock_pool_sync.py` 已能识别 `index_pool__*` 和 `filtered_pool__index_union__*`，可作为薄适配基础；
- `ExperimentConfig.stock_pool` 仍是字符串，不能表达单指数、指数并集与同策略多池对比的不同语义；
- MCP 主要转发自由结构 `config_json`，UI 仍有硬编码日期，用户无法通过一致的人类可读控件完成选择。

当前需要收敛的默认日期入口如下：

| 当前入口 | 当前问题 | 本阶段处置 |
|---|---|---|
| `config_composer.py` | signal/backtest end 固定为 2026-06-30/29 | 改读 resolved profile |
| `qe_templates/validator.py` | 模板缺省仍为 2026-04-28/27 | 改读同一 profile summary |
| `quantevolver.py` / `quantevolver_evolution.py` | 部分 QE 评估/变换入口仍固定 2026-06-30 | 属于实际 QE 回测的入口改读 profile |
| QE compose page | 前端固定 2026-06-30/29 | 由只读 profile API 初始化 |
| MultiAlpha create composer | OOS end 固定为 2026-06-29 | 由 profile backtest end 初始化 |
| QE factor correlation/cache UI | 展示/请求固定 2026-06-30 | 若仅为官方因子库统计则保留原合同并登记后续 adapter；若触发 QE 回测则必须使用 profile |

本阶段的判断标准不是文件位于哪个目录，而是该入口是否创建 QE 训练、预测或回测。实际执行 QE 的入口全部收敛；纯因子库
离线统计的窗口不被本设计顺带改变，避免把本功能扩大为因子库迁移。

### 1.3 设计原则

本设计只建立一条 QE 消费链：

```text
一个活动数据集 profile
        │
        ├─ 默认 release / cutoff / data split
        ├─ WSL 与远端节点的同 release 路径
        └─ 可用 PIT 股票池 sidecar identity
        │
        ▼
任务创建时 resolve once
        │
        ▼
不可变 run-scoped binding + 最终生成配置
        │
        ▼
训练 / 预测 / 分钟线回测（只读 Bin/H5/Parquet/sidecar，不查业务数据表）
```

活动 profile 是“新 QE 任务的默认解析来源”，不是历史实验重写器，也不是跨模块 production dataset pointer。
后续因子库或其他模块需要统一数据集时，可以通过新 schema 版本扩展同一物理配置文件；本阶段只实现 `consumers.qe`，
不提前建设通用配置平台。

## 2. Scope / 范围

### 2.1 Goals / 目标

- 建立一个仓库外、无秘密、可原子替换的活动数据集 profile，月度更新只修改这一份文件内容。
- 活动 profile 同时提供 release identity、cutoff、QE 默认日期、节点数据根、组件 pins 和可用股票池。
- UI、REST、Codex/Claude MCP、单次实验、自定义演进、策略演进和多 Alpha 创建入口使用同一解析器。
- 新请求未填写日期时使用活动 profile 的最新默认窗口；填写合法日期时允许在该 release 覆盖范围内覆盖。
- 未选择股票池时保持 `stock_universe`；显式支持单指数和多个指数 PIT 并集。
- 同一策略可以展开为多个股票池实验臂，除 universe 外模型、因子、seed、日期、执行算法和费用保持一致。
- WSL 与 `rdagent-node1` 使用同一 release identity、同一 run-local sidecar content hash，但解析各自 candidate 绝对 POSIX 路径。
- 每个任务创建时把解析结果持久化；重试、恢复、重放和历史读取继续使用原 binding，不跟随活动 profile 漂移。
- 训练和回测数据面保持零数据库访问；数据库继续只记录任务、状态、结果和控制面元数据。
- 缺少 profile、节点 candidate、控制端 sidecar、hash、日期覆盖或选中窗口物理数据时返回稳定 reason code，不回落旧路径或默认池。

### 2.2 Non-Goals / 非目标与边界

- 不修改、移动、覆盖或重新导出 2026-08-31 candidate 的 Bin/H5/Parquet 大组件。
- 不在 QE 进程中查询 `market.core_index_membership_pit`、`market.stock_universe_pit_*` 或其他业务数据表。
- 不自动下载或补齐缺失行情，不让 QE 窗口承担数据准备职责。
- 不修改模型、因子、label、seed、分钟线 TWAP、费用、Top-K 或策略退出业务语义。
- 不把多选指数自动解释成多个实验；“指数并集”和“分别对比”必须是两个显式操作。
- 不为每个节点、页面或 MCP 复制一份默认日期配置。
- 不新增通用配置数据库、配置中心、守护进程、watcher、心跳、事件表或定时轮询。
- 不新增手工 signoff、研究审批、全历史 hash、每次实验全目录复扫或资源利用率门禁。
- 不改变已创建任务、历史模板、历史策略包和归档实验的 binding。
- 不在本设计 PR 执行 DDL/DML、候选写入、sidecar 部署、生产配置切换或任何进程启停。

## 3. Architecture / 总体架构

### 3.1 唯一活动 profile

生产环境通过稳定环境变量 `AISTOCK_ACTIVE_DATASET_PROFILE_PATH` 指向一个仓库外绝对文件；该变量是唯一位置权威，
不得再从当前目录或其他环境变量猜测 profile 文件。环境变量只在首次上线时
配置，月度更新不改 `.env`，只原子替换目标文件。

建议目标值：

`<RDAGENT_STATE_ROOT>/dataset_profiles/active_dataset_profile.json`

实际 authority 仍是环境变量解析出的规范绝对路径。reader/activate CLI 必须拒绝仓库内路径、相对路径、symlink、junction、
reparse point 和非普通文件；建议位置只是部署约定，不构成第二个 fallback。

文件 schema 为 `aistock_active_dataset_profile_v1`。本阶段只读取其中 `consumers.qe`；未来其他消费者需要接入时通过
schema 升级和各自 adapter 实施，不允许 QE parser 宽松接受未知语义。

活动 profile 保持小型，仅保存身份、路径、日期、hash 和紧凑覆盖摘要，不嵌入成员全量、因子数据、日志或数据库凭据。

`profile_sha256` 不写回 profile，避免自引用；reader 对 UTF-8 canonical JSON 的完整文件字节计算 SHA-256，并把结果写入
resolved summary 和 run binding。`generation` 是数据准备侧随一次活动配置发布生成的非空稳定字符串，同一 generation 的
文件内容必须保持不变；内容变化必须产生新 generation。

### 3.2 读取与生效时点

- 普通 single/custom/strategy 创建入口在创建任务时同步读取并严格验证；模板只保存语义请求，在 materialize 时解析一次；
  create-and-run 在同一事务流程中解析一次。每条路径只能有一个解析时点。
- 同一次任务只解析一次，生成 `resolved_at`、profile digest 和不可变 run binding。
- worker/qrun 不重新读取活动 profile，防止运行中月更改变任务语义。
- 已创建任务的 retry/resume/rerun 从持久化 binding 恢复，不读取当前默认值。
- 原子替换成功后，下一个新任务自然看到新 generation，无需轮询数据库。

原子写入只由 `scripts/qe_active_dataset_profile.py validate|activate` 提供：`validate` 只读；`activate` 在用户授权的精确
source/target 上执行同目录临时文件、flush/fsync 和原子 replace，并在替换前后校验 digest。它不是配置服务，不启动进程、
不修改候选数据，也不加入实验创建路径。

### 3.3 兼容启用

源码合入但尚未设置 `AISTOCK_ACTIVE_DATASET_PROFILE_PATH` 时，现有任务和当前默认行为保持不变，API 明确返回
`mode=legacy_default_not_activated`。这只用于源码先行部署，不是静默错误。

正式上线时设置稳定 profile path 并由用户执行所需 backend 重启。启用后，文件缺失、schema 非法或内容漂移会阻断
新任务创建；不会回退到硬编码日期、旧 `/home/lc999/data/qlib_bin` 或 `stock_universe`。

### 3.4 组件边界

| 组件 | 职责 | 禁止职责 |
|---|---|---|
| `qe_active_dataset_profile.py` | 读取、验证、摘要、日期与节点 binding 解析 | 数据导出、DB 查询、远端复制、进程控制 |
| `scripts/qe_active_dataset_profile.py` | profile 只读验证与精确原子切换 | 自动激活、目录扫描、数据复制、服务重启 |
| `qe_dataset_contract.py` | v2 历史 binding 兼容、v3 新 binding 与 universe identity | 读取活动 profile、隐式选择节点 |
| `ExperimentConfig` / template validator | 接受语义化日期和 universe 请求 | 接受客户端绝对路径、hash 或内部 binding |
| `ConfigComposer` | 使用已解析 binding 生成 Qlib/RD-Agent 配置 | 读取数据库、根据文件存在性猜测旧路径 |
| workspace payload / run-local day overlay | 携带已钉 hash 的小型 sidecar并在任务目录建立只读 provider 视图 | 修改来源 candidate、查询成分数据库 |
| REST/UI/MCP | 展示默认值、提交语义化覆盖 | 显示或要求用户输入 JSON、文件路径、hash、内部 ID |

## 4. Contracts / API、配置、UI 与 MCP 契约

### 4.1 活动 profile 字段

| 区域 | 必需字段 | 语义 |
|---|---|---|
| identity | schema version、generation、release id、cutoff、profile digest | 一次月度默认配置身份 |
| components | daily/minute/factor/index/suspend 的相对路径和 content pins | 同一 release 组件身份，不允许混版 |
| node bindings | `wsl2-5080`、`rdagent-node1` 的 candidate root | 只允许已登记节点；组件路径从 root 推导 |
| QE defaults | train/valid/test 起止、signal end、backtest end、默认 universe | 新任务未覆盖时使用 |
| universes | pool id、中文名、mode、primitive sidecar 文件名/hash、membership revision | UI/MCP 可选目录 |
| coverage | 组件覆盖截止、缺口 receipt ref/digest、可用窗口摘要 | 小文件判定，不在每次请求扫描大目录 |

`backtest_end` 必须是 release 日历中 `signal_end` 的前一个可用交易日，而不是简单减一天。对 2026-08-31 release，
默认 `signal_end/test_end=2026-08-31`，默认 `backtest_end=2026-08-28`。训练、验证和测试起点继续采用经批准的
现有 split，除非请求显式覆盖。该结论已由 candidate `calendars/day.txt` 末尾
`2026-08-25,26,27,28,31` 的只读回读确认。

节点 binding 只保存两个根路径；daily、minute、factor、index 和 suspend 路径继续由 direct-v2 目录合同推导，避免
同一节点重复维护五个路径。

### 4.2 日期优先级与约束

新任务的日期解析顺序：

1. 请求中显式、结构化的 `data_split` / `oos_start` / `oos_end`；
2. 活动 profile 的 `consumers.qe.defaults`；
3. 未启用 profile 时的历史兼容默认值。

规则：

- 显式日期必须处于活动 release `[start, cutoff]` 内并保持 train < valid < test 顺序；
- 未填写 `test_end` 时取 profile `signal_end`；
- 未填写 `backtest_end` 时，若 `test_end=signal_end`，取 profile `backtest_end`；若选择更早 `test_end`，可使用同日，
  因为 provider 中存在后续日历行；
- `backtest_end` 不得晚于 `test_end`；
- 多 Alpha `oos_end` 默认取 profile `backtest_end`，但不得超过组成腿的预测覆盖交集；
- `signal_end` 表示可生成预测的最后交易日；`backtest_end` 表示在当前 minute 数据中可完成执行的最后信号日；
- `outcome_observable_end` 按 label horizon 从 release 日历反向推导，表示未来收益已完整落入 release 的最后信号日；
- IC、RankIC、Top-K future return 等依赖未来结果的指标必须报告实际 `outcome_observable_end` 和有效样本数，不能把
  `signal_end=2026-08-31` 表述为 h20 结果已经观察到 2026-08-31；
- 页面初始化值只能来自 profile summary API，不再维护日期常量；
- 服务端永远重新解析并持久化最终值，不能信任前端默认值。

稳定错误：

- `qe_active_dataset_profile_missing`
- `qe_active_dataset_profile_invalid`
- `qe_dataset_node_binding_missing`
- `qe_dataset_window_outside_release`
- `qe_dataset_component_identity_mismatch`

### 4.3 UniverseSelection

公共语义对象只有三种 mode：

| mode | pool_ids | 结果 |
|---|---|---|
| `stock_universe` | 空 | canonical 全市场股票池 |
| `single_index` | 恰好一个 | 该指数 PIT sidecar |
| `index_union` | 一个或多个 | primitive sidecar 文件级区间并集、排序和去重 |

规则：

- `pool_ids` 使用稳定公开代码，如 `csi300`，不要求用户输入数据库 ID、指数代码或文件名；
- pool ids 排序去重，`index_union` 对输入顺序不敏感；
- 多指数 union 在任务创建期只读取已钉 hash 的 primitive sidecar，确定性生成 run-local sidecar；不查询数据库；
- 同一股票的重叠区间合并，训练样本、预测和订单候选只出现一次；
- benchmark 独立配置，不能把第一个 pool 隐式作为 benchmark；
- 未提供 `universe_selection` 的新任务取 profile 默认，初始默认仍为 `stock_universe`；
- 旧任务的 `stock_pool` 字符串保留只读复现；新 API 若同时收到 `stock_pool` 和 `universe_selection` 则拒绝冲突。

稳定错误：

- `qe_universe_mode_invalid`
- `qe_universe_pool_unknown`
- `qe_universe_sidecar_not_deployed`
- `qe_universe_sidecar_hash_mismatch`
- `qe_universe_window_coverage_incomplete`

### 4.4 Run-scoped binding v3

新任务使用 `qe_direct_v2_dataset_binding_v3`，保留 v2 全部组件 identity，并把 selection 扩展为：

- `mode`
- 排序去重后的 `pool_ids`
- `instrument_name`
- `instruments_file`
- `instruments_sha256`
- `membership_revision`
- `coverage_receipt_sha256`
- 独立 `benchmark_code` 与 `benchmark_instruments_sha256`

v2 binding 继续用于历史任务复现，只能表达 `stock_universe`。v3 不能由 v2 在运行时猜测升级；新任务必须由活动
profile resolver 生成。

生成 workspace 时：

- `stock_universe` 直接使用 candidate 自带文件；
- `single_index` 在控制端校验 primitive sidecar hash 后，将精确文本随既有 workspace payload 写入任务目录；
- `index_union` 在控制端校验所有 primitive sidecar，文件级合并并计算 run-local hash 后随同一 payload 写入；
- 配置组装器在任务目录建立仅含选中 instruments 的 `qe_provider_day` 视图，calendars/features/meta 只链接已绑定 candidate，
  不写 candidate，也不依赖节点侧第二份 sidecar 部署；
- frozen risk policy 使用 binding 指定的 instruments，不再限制为两个固定文件名；
- qrun fresh process 再验证 binding、组件路径、sidecar hash 和覆盖 digest；
- 任一失败停止该任务，不回落默认池或旧数据目录。

### 4.5 REST API

新增只读摘要：

- `GET /api/v1/quantevolver/dataset-profile`

返回 UI/MCP 所需的 release label、cutoff、默认日期、可用节点、默认股票池和人类可读股票池目录；不返回本地绝对路径、
完整 pins 或内部绑定。

现有创建入口增加可选语义字段：

- `universe_selection`
- 已有 `data_split` 或 OOS 日期继续沿用

新增 `POST /api/v1/quantevolver/evolution/universe-comparison-tasks`，把一个公共 Loop 配置和至少两个 `pool_ids`
展开为现有 custom-evo 的独立实验臂；它不新增执行器或调度器。

响应及任务详情增加：

- resolved release id / generation / cutoff
- resolved dates
- label-horizon 对应的 outcome-observable end 与有效样本数
- resolved universe label / mode / pool ids

binding digest、绝对路径和 pins 只在内部持久化与 fresh-process 校验中使用，不返回给 UI/MCP。

不新增 profile 写 API。活动 profile 切换是独立、精确授权的运维动作，避免把全局默认值暴露为普通 UI 写接口。

### 4.6 UI

QE 创建页增加一个“数据与股票池”区块：

- 数据集：只读显示当前 release 和截止日期；
- 日期：默认带出 profile 日期，仍允许使用日期控件覆盖；
- 股票池模式：全市场、单指数、多指数并集；
- 单指数使用单选下拉；多指数并集使用带中文名称的复选框；
- “分别对比”是独立开关，开启后将所选 pool 展开为独立实验臂，不与并集混淆；
- 展示当前窗口覆盖状态；不可用时显示 reason code、受影响组件、数量和修复建议；
- 禁止 JSON 编辑框、内部 ID、文件路径、hash 或手工 sidecar 文件名输入。

前端只负责采集语义选择。可用性、日期上限和最终 binding 由服务端重新验证。

### 4.7 MCP / Codex / Claude Code

MCP 增加只读工具 `qe_dataset_profile_get`、结构化单实验模板工具
`qe_single_experiment_template_create` 和独立对比工具 `qe_universe_comparison_task_create`，参数包括：

- `universe_mode`
- `pool_ids`
- 可选日期字段
- union 由 `universe_mode=index_union` 表达；separate-runs 由独立对比工具名显式表达，不根据数组长度猜测

MCP 仍然是 loopback API 薄封装，不导入 QE scheduler、不读取节点文件、不直接拼装 internal binding。用户和 agent 不需要
提交原始 JSON、路径、hash 或实验内部 ID。服务端回传 resolved summary，便于 agent 在真正运行前展示将使用的 release、
日期和股票池。

### 4.8 同策略多股票池对比

每个实际 run/loop 只能有一个 `UniverseSelection`。`separate_runs` 复用现有 custom-evo/template 批量物化能力，将选择展开为
多个独立实验臂，不新增第二个调度器。

对比组必须锁定以下字段：

- release generation、数据 split 和评估窗口；
- 模型、因子、超参数、label 和 seed；
- 分钟线 TWAP、费用、Top-K、持仓和退出策略；
- 执行节点能力合同。

唯一允许变化的主变量是 universe。系统保存 comparison group id 和 arm label，但不新增人工晋级门禁。

需要明确区分两类实验：

1. **已有预测过滤对比**：同一份全市场模型/prediction 按不同 PIT sidecar 过滤并回测，回答模型在不同 universe 的可迁移性；
   该方式不能宣称模型在各池内经过专用训练，也不能补出源 prediction 不存在的证券。
2. **股票池专用训练对比**：各实验臂从 train/valid/test 到预测、回测都绑定同一 universe，回答专用模型的真实效果；
   该方式需要完整历史物理覆盖，成本更高。

第一轮先做已有预测过滤对比；只有出现稳定分层后才启动专用训练，避免无证据地复制大批训练任务。

第一轮建议同时输出两种口径：固定 Top20/Top50 的部署口径，以及按股票池规模归一化的 Top 分位诊断口径。否则科创50
的 Top50 与全市场 Top50 不具有相同选择强度。评价至少包含 IC、RankIC、Top-K 收益/命中率、CAGR、Sharpe、Calmar、
MDD、换手、成本、容量、相对对应指数的主动收益/IR、每日 eligible count 和跨池 Top-K 重叠率。

所有实验臂还必须使用共同的 `outcome_observable_end` 做结果比较；较晚但尚无完整未来收益的数据只进入预测覆盖报告，
不进入已实现收益或 IC 优劣结论。

## 5. 数据缺口与数据准备交接

### 5.1 当前缺口的准确含义

2026-09-05 生产完整校验顶层为 PASS，但 `physical_coverage` 明确报告：

- 日线缺少 131 个历史证券文件；
- 分钟线缺少 131 个历史证券文件；
- 两者并集为 142 个证券，其中 120 个同时缺日线和分钟线；
- 按股票池去重统计：中证1000 涉及 96 个、中证500 涉及 50 个、沪深300 涉及 14 个，科创50/科创100为 0；
- 这些集合跨池有重叠，不能相加作为总数；
- 2026-08-31 当日没有仍在池内的上述缺失证券，因此最新截面烟测不被这一缺口阻断。

这不是指数成分 authority 缺失，而是部分历史成员在当前 daily/minute candidate 中没有物理 feature 文件。

### 5.2 数据准备窗口需要处理的精确范围

若要执行从 2018-08-01 开始的指数池专用训练或完整历史回测，数据准备窗口需要针对上述 142 个证券，仅在其
`指数成员 PIT ∩ canonical equity PIT ∩ [2018-08-01, 2026-08-31]` 区间内检查并补齐：

1. direct-v2 日线必需字段；
2. 分钟线 TWAP 所需的 1min OHLCV/amount 及现有执行合同字段；
3. 仅对实际训练因子检查对应 H5/static coverage；
4. suspend/limit 等现有严格执行输入的同区间覆盖。

不得补成员区间之外的数据，不得前填、补零、用当前成分代替历史成分或重导全市场。

若第一轮只做 2024-07-01 至 2026-08-31 的刷新回测，当前已知精确影响范围缩小为 9 个证券：

| pool | 证券 | 有效成员区间 | 已知缺口 |
|---|---|---|---|
| 中证1000 | `000627.SZ` | 2021-06-15～2025-06-13 | daily |
| 中证1000 | `000982.SZ` | 2022-06-13～2024-08-02 | daily + minute |
| 中证1000 | `300379.SZ` | 2018-08-01～2025-04-30 | daily |
| 中证1000 | `600225.SH` | 2023-06-12～2024-10-31 | daily + minute |
| 中证1000 | `600297.SH` | 2024-06-17～2024-07-22 | daily + minute |
| 中证1000 | `600811.SH` | 2022-12-12～2024-12-13 | daily + minute |
| 中证1000 | `601028.SH` | 2021-06-15～2025-04-11 | daily |
| 沪深300 | `600837.SH` | 2018-08-01～2025-01-27 | daily + minute |
| 沪深300 | `601989.SH` | 2018-08-01～2025-08-06 | daily |

数据准备侧应先生成精确缺口清单和覆盖 receipt，再对这些证券/日期增量处理；本设计不预判缺失源能否取得，也不授权执行。

### 5.3 必须补齐的理由

- 历史指数池若静默漏掉退市或退出指数的成员，会形成幸存者偏差；
- 不同股票池漏失比例不同，会把数据完整性差异误判为 alpha 差异；
- 缺日线会影响训练、label、benchmark-relative 归因和预测；
- 缺分钟线会让 TWAP 成交能力、涨跌停/停牌限制、成本和收益不可比；
- 用空值、前填或缩短历史窗口不能恢复真实成员当时的可交易路径。

### 5.4 覆盖 receipt 合同

数据准备窗口的完整签收证据继续独立保存 candidate digest、primitive sidecar hash、checked interval、缺口明细引用、
eligible count 与 source/candidate/production writes；它不直接进入每个 QE task。

QE 消费的是从该证据确定性导出的紧凑 coverage projection，schema 固定为
`qe_index_pool_coverage_receipt_v1`，顶层只含 `schema_version/release_id/cutoff/pools`；每个 pool 只含
`available_start/available_end/gaps`，每个 gap 只含 `symbol/start/end/components(day|1min)`。profile 钉住该投影的
SHA-256，并在 components/universes 区域分别钉 candidate 与 sidecar identity。因此 QE 不复制完整签收报告，也不在每次创建
任务时重新扫描 38GB 分钟线目录。

数据补齐不阻断 QE 源码开发和 fixture 验证；它只阻断与缺口区间相交的正式指数池训练/回测。最新截面和不相交窗口可以在
活动 profile 激活后进行短烟测。

活动 profile 可以在默认 `stock_universe` 可用时上线，并把存在历史缺口的指数池标记为“按窗口判断”；不要求为启用全局
日期默认值先补齐全部 142 个历史证券。只有请求窗口与精确缺口相交时，相关指数池实验被拒绝并返回清单引用。

### 5.5 sidecar 控制端输入与双节点传输范围

实现审核将“每个节点再维护一份 stock-pool root”识别为不必要的第二部署面。权威输入仍是数据准备窗口已经生成的五个
`index_pool__*.txt` primitive sidecar 与一个 compact coverage receipt；backend 在任务创建时校验普通文件属性和
SHA-256，任意 union 在内存中确定性生成，并通过现有 QE workspace payload 只发送本次任务所需的一个 sidecar 和 receipt。

因此：

- WSL 与 remote 只需各自已有的同 release candidate root；
- profile 的 node binding 只登记 candidate root，不登记或猜测 stock-pool root；
- sidecar 不写入 candidate，不需要额外远端同步，也不要求重启节点 API；
- qrun 只读取 run workspace 与节点 candidate，不访问 Windows 路径或数据库；
- 禁止递归复制整个 X 盘 candidate、覆盖 daily/minute/factor 组件或建立跨 release fallback。

## 6. Implementation Plan / 实施方案

源码以一个 QE feature PR 交付，保持配置、API、UI 和 MCP 同步，避免出现后端已支持但用户仍需手填 JSON 的中间状态。

### Phase 1：profile 与 binding 核心

1. 新增 `backend/services/quantevolver/qe_active_dataset_profile.py`：严格 schema、摘要和 node-specific resolver。
2. 新增 `scripts/qe_active_dataset_profile.py`：只读 validate 与授权后的精确 atomic activate。
3. 在 `qe_dataset_contract.py` 增加 v3 binding，保留 v2 历史读取。
4. 扩展 `ExperimentConfig` 为语义化 `universe_selection`，定义与 legacy `stock_pool` 的冲突规则。
5. 修改 `ConfigComposer`：默认日期来自 resolved profile，single/union 使用 binding 指定 instruments。
6. 扩展 direct-v2 validator、risk-policy builder 和 workspace payload 的精确 sidecar 合同。

### Phase 2：所有 QE 创建入口收敛

1. 单次实验、custom-evo、strategy-evo、template materializer 使用同一 resolver。
2. 多 Alpha combine 的默认 OOS end 来自活动 profile，并验证组成腿覆盖交集。
3. QE 内执行回测的 factor transform/evaluation 默认截止也读取同一 profile；纯因子库月度计算只预留后续 adapter，
   不在本阶段改变其业务合同。
4. 删除 QE 新任务路径中的日期硬编码；历史兼容 marker 只服务旧 payload 识别。

### Phase 3：REST、UI 与 MCP

1. 增加只读 profile summary API。
2. 创建页增加日期与人类可读股票池控件，并支持 union/separate-runs 的显式区别；separate-runs 复用 custom-evo，
   服务端生成 comparison group identity 和各 arm label，不新增调度器或 DDL。
3. MCP 增加 profile read 和结构化创建参数，继续薄转发 loopback API。
4. 任务详情和结果页展示 resolved release/date/universe，不展示内部路径和完整 binding。
5. 公开创建入口拒绝客户端注入 server-owned binding、路径、hash、sidecar content 或 coverage receipt；
   已持久化任务的编辑/重跑在服务端复用原 binding，不重新读取活动 profile。

### Phase 4：上线前验证与激活

1. fixture profile 覆盖 WSL/remote 路径、v2/v3、日期覆盖、single/union、缺口和 tamper 负向测试。
2. 源码合入后，由用户重启 backend-main，再做只读 identity/business smoke。
3. 经用户对精确 profile 文件授权后，原子写入活动 profile；不修改 candidate 大组件。
4. 分别在 WSL 和远端运行一个短 single-index smoke；workspace payload 携带本次所需 sidecar。
5. 短 smoke 通过后才启动同策略多池对比。

源码预计 runtime impact 为 `backend + frontend + client/MCP`：backend-main 重启、frontend activation 与 MCP 客户端/进程
重新加载必须分别报告；它们互不推导授权。计算节点只消费新 workspace payload，若不修改节点 API/release，本功能不要求
重启 WSL 或远端 API。

## 7. Verification Plan / 验证方案

### 7.1 核心合同测试

- `backend/tests/quantevolver/test_qe_active_dataset_profile.py`
  - schema、digest、日期、两个节点根路径和组件派生；
  - profile 未启用时兼容模式；启用后缺失/非法不回落；
  - 原子替换后只影响新任务，已解析 binding 不漂移。
- `backend/tests/quantevolver/test_qe_active_dataset_profile.py`
  - validate 零写入；activate 只替换精确 target；digest/schema 不符不改变旧文件；
  - 路径必须为普通文件且位于仓库外，拒绝 symlink/junction 和目标漂移。
- `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py`
  - v2 历史 binding 保持可读；
  - v3 stock-universe/single/union identity；
  - benchmark 独立、node/release/component 混版拒绝。
- `backend/tests/quantevolver/test_qe_active_dataset_profile.py`
  - pool id 规范化、union order-invariant、重叠区间合并；
  - primitive/union hash tamper、缺 sidecar和覆盖缺口拒绝；
  - 无数据库驱动导入或 SQL fallback。

### 7.2 创建与配置生成测试

- `backend/tests/unified_engine/test_qe_config_truth.py`
  - 单次、custom、strategy、新模板都解析相同默认日期；
  - 显式日期覆盖和越界拒绝；
  - WSL/remote 生成配置只使用对应 node root；
  - 配置中无 `/home/lc999/data/qlib_bin`、旧 minute/factor path 或 `/mnt/x` 泄漏到远端。
- `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py`
  - 精确文件名、普通文件、hash、run-local day provider 与 fresh-process v3 校验；
  - 不修改来源 candidate。
- `backend/tests/qe_templates/test_template_validator.py`
  - UI/MCP template 省略日期时使用活动 profile；
  - retry/rerun 使用原 binding；
  - legacy stock_pool 与新 selection 冲突可见。
- `backend/tests/unified_engine/test_multi_alpha_command_generation.py`
  - OOS default、prediction coverage intersection 和 separate-runs 参数不变性。

### 7.3 API、UI 与 MCP 测试

- `backend/tests/quantevolver/test_qe_active_dataset_profile.py`：摘要无绝对路径/内部 hash，错误结构稳定。
- `backend/tests/test_aistock_qe_mcp_servers.py`：MCP profile read、结构化日期/pool 参数和 loopback 薄封装。
- 前端 typecheck/CI 与创建页契约检查：默认值、日期覆盖、单选、多选并集、分别对比、禁止 JSON/ID 输入和错误展示。
- 浏览器验收只覆盖用户可见创建流，不启动正式训练；长任务与真实全量回归交由后续实验任务。

### 7.4 静态与流程验证

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_active_dataset_universe_management_f2_design_20260906.md --tier F2`
- `python -m nox -s validation_module_registry_l0`
- `python -m nox -s l0`
- changed-files ownership/scope check
- `git diff --check`

### 7.5 真实上线 smoke

上线后只运行两个短 smoke：

1. WSL：活动默认日期 + `single_index/csi300`；
2. `rdagent-node1`：同 profile generation + `index_union/csi300,csi500`。

两者验证最终配置、release id、cutoff、sidecar hash、实际 provider URI、分钟线 TWAP 和数据集写入数为 0。它们不构成
正式 alpha 结论，也不要求重复全历史数据校验。

## 8. Rollout / Rollback / 发布与回滚

### 8.1 发布顺序

1. 合入本详细设计；
2. 实现并验证 QE feature source PR；
3. 用户合入并重启 backend-main，验证源码身份；
4. 用户授权精确活动 profile 文件，执行一次原子切换；
5. WSL/remote 短 smoke，验证 workspace payload 的 sidecar 与绑定 hash；
6. 开始同策略多股票池实验。

### 8.2 回滚

- profile 切换失败：原子操作前不改变当前文件；失败返回错误，不生成半文件。
- 新 profile 业务异常：将活动文件原子切回上一份已验证 profile；已运行任务继续使用原 binding。
- 源码异常：revert feature merge commit，用户重启 backend-main；数据和控制端 sidecar 保持只读不删除。
- 不通过修改历史实验、覆盖 candidate、恢复旧 `/home/lc999/data/...` 或改写数据库实现回滚。

## 9. Production Gates / 生产边界

| 状态项 | 本设计 PR | 后续实现/上线 |
|---|---|---|
| DEV DB DDL/DML | `noop` | `noop`，本功能不需要数据库 schema |
| Production DB DDL/DML | `noop` | `noop` |
| 大数据组件写入 | `noop` | `forbidden` |
| sidecar/coverage 节点部署 | `noop` | `noop`；由既有 workspace payload 传输本次所需小文件 |
| source merge | 未执行 | 用户单独确认 |
| backend restart | `backend_restart_owner=false` | 源码合入后由用户执行 |
| frontend activation | `noop` | 用户单独执行或确认既有发布流程 |
| MCP client/process reload | `noop` | 新工具需要时独立处理，不由 source merge 推导 |
| 活动 profile 切换 | `noop` | 用户授权精确文件后执行 |
| 实验启动 | `noop` | 双节点 smoke 通过后另行执行 |
| cleanup/delete | `noop` | 本功能不需要删除数据 |

profile schema、hash、日期和 selected-window coverage 是输入正确性检查，不是新的人工审批。禁止把 UI signoff、资源利用率、
全历史复扫、重复 smoke 或实验收益阈值增加为上线前置门禁。

## 10. Risks / Failure Modes / 风险与失败模式

| 风险 | 处理 | 禁止做法 |
|---|---|---|
| profile 与节点 candidate 不一致 | node-specific binding + fresh-process hash 校验 | 回落旧路径 |
| 月更中任务语义变化 | 创建时 resolve once 并持久化 binding | worker 每轮读取 active profile |
| 硬编码日期仍残留 | QE 创建入口 inventory 测试 | 逐页面继续改常量 |
| 多选语义混淆 | union 与 separate-runs 独立字段/控件 | 根据数组长度猜语义 |
| 多池包含重复股票 | 区间并集和去重 | 重复训练、预测或下单 |
| benchmark 被池选择污染 | 独立显式 benchmark | 默认取首个指数 |
| 历史缺行情产生幸存者偏差 | coverage receipt + 精确窗口拒绝 | 丢股票、缩窗口、前填、补零 |
| 每次请求扫描 38GB 分钟线 | 消费紧凑 coverage receipt | 全目录 rescan |
| 客户端伪造路径/hash | 只提交 semantic selection，服务端解析 | 暴露 raw binding 输入 |
| 未来模块扩展污染 QE | schema 版本化、消费者 adapter 分离 | 现在建设通用配置平台 |

## 11. Design Acceptance Index / 设计验收索引

| 编号 | 设计要求 |
|---|---|
| F-001 | QE 新任务的 release、cutoff、默认日期、节点根和可选股票池来自一个活动 profile |
| F-002 | 月更只原子替换一份仓库外 profile，不需要修改后端/前端日期常量或多个 `.env` 路径 |
| F-003 | profile 只在任务创建/materialize 时解析一次，retry/resume/rerun 使用原 binding |
| F-004 | 未启用 profile 时保持历史兼容；启用后缺失/非法配置 fail closed 且不回落 |
| F-005 | 默认 test/signal end 等于 release cutoff，backtest end 来自前一交易日 |
| F-006 | UI/MCP 可以显式覆盖 release 范围内的日期，越界请求被拒绝 |
| F-007 | 支持 stock-universe、single-index 和 index-union 三种 universe mode |
| F-008 | 多指数并集由已钉 hash 的 primitive sidecar 文件级确定性生成，不查询数据库 |
| F-009 | benchmark 与 universe 独立，指数代码不进入选股 instruments |
| F-010 | v2 binding 继续复现历史任务，v3 binding 完整记录 universe、coverage 和组件身份 |
| F-011 | WSL/remote 使用同 release/hash 和各自 node root，禁止跨节点路径泄漏 |
| F-012 | direct validator、risk-policy builder 和 workspace payload 共同支持安全 run-local sidecar |
| F-013 | 单次、custom、strategy、template 和 multi-alpha 创建入口使用同一默认解析器 |
| F-014 | UI 只提供人类可读日期及股票池控件，不出现 JSON、路径、hash 或内部 ID 输入 |
| F-015 | MCP 保持 loopback 薄封装并提供结构化日期、pool 和 comparison 参数 |
| F-016 | 同策略多池对比只改变 universe，其他模型、因子、seed、执行和成本参数固定 |
| F-017 | union 和 separate-runs 语义显式分离，不根据多选数量猜测 |
| F-018 | selected-window 覆盖缺口返回稳定错误，不静默丢弃证券或缩短窗口 |
| F-019 | 当前 142 个历史缺失证券按 PIT 成员区间精确交接，不触发全市场重导 |
| F-020 | 活动 profile 引用紧凑 coverage receipt，不在每次任务创建时扫描大数据目录 |
| F-021 | QE 训练/回测数据面只读文件，不访问业务数据表 |
| F-022 | 不新增配置数据库、daemon、watcher、heartbeat、事件表或人工审批门禁 |
| F-023 | 源码、profile 切换、重启和实验启动保持独立状态与授权；节点 sidecar 部署为 noop |
| F-024 | 回滚只切回已验证 profile 或 revert 源码，不覆盖 candidate 或改写历史任务 |
| F-025 | 实施逐项满足 DESIGN-COMPLIANCE-001，并以最终 HEAD 的测试和证据验收 |
| F-026 | signal、execution/backtest 与 label outcome 可观测截止分离并在结果中真实报告 |

## 12. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `qe_active_dataset_profile.py`；§§3.1,4.1 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | design_review_pass | none |
| F-002 | profile reader + `scripts/qe_active_dataset_profile.py`；§§3.1,3.2 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | implementation_review_pass | none |
| F-003 | resolver + persisted binding；§3.2 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py`; `backend/tests/unified_engine/test_backtest_executor.py` | implementation_review_pass | none |
| F-004 | compatibility mode；§3.3 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | design_review_pass | none |
| F-005 | date resolver；§§4.1,4.2 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | design_review_pass | none |
| F-006 | REST request validation；§4.2 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | implementation_review_pass | none |
| F-007 | `UniverseSelection`；§4.3 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | implementation_review_pass | none |
| F-008 | file-only union builder；§§4.3,4.4 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | implementation_review_pass | none |
| F-009 | selection/benchmark pins；§4.4 | `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py` | implementation_review_pass | none |
| F-010 | `qe_dataset_contract.py` v2/v3；§4.4 | `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py` | implementation_review_pass | none |
| F-011 | node-specific resolver；§§3.4,4.1 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py`; `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py` | implementation_review_pass | none |
| F-012 | validator/risk-policy/workspace payload；§§4.4,5.5 | `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py` | implementation_review_pass | none |
| F-013 | ExperimentConfig/template/evolution/combine adapters；§6 | `backend/tests/qe_templates/test_template_validator.py`; `backend/tests/unified_engine/test_multi_alpha_command_generation.py` | implementation_review_pass | none |
| F-014 | QE create UI controls；§4.6 | `backend/tests/quantevolver/test_qe_dataset_universe_frontend_contract.py`; frontend typecheck/CI | implementation_review_pass | none |
| F-015 | MCP wrapper；§4.7 | `backend/tests/test_aistock_qe_mcp_servers.py` | implementation_review_pass | none |
| F-016 | custom-evo comparison expander；§4.8 | `backend/tests/quantevolver/test_qe_universe_comparison.py` | implementation_review_pass | none |
| F-017 | explicit comparison mode；§§4.6,4.8 | `backend/tests/quantevolver/test_qe_universe_comparison.py`; frontend typecheck/CI | design_review_pass | none |
| F-018 | coverage resolver；§§5.1,5.4 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | implementation_review_pass | none |
| F-019 | exact data handoff；§5.2 | `artifact: X:/AIstock_dataset_candidates/core_index_membership_pit/production-final-full-validate-20260905.json` | design_review_pass | none |
| F-020 | compact coverage reference；§5.4 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | design_review_pass | none |
| F-021 | qrun DB poison/static isolation；§§2.1,4.4 | existing `backend/tests/multi_alpha/test_qe_subprocess_db_isolation.py`; static review of new stdlib-only resolver | design_review_pass | none |
| F-022 | §§2.2,3.2,9 | `python -m nox -s l0` | design_review_pass | none |
| F-023 | §§8,9 | `python -m nox -s validation_module_registry_l0` | design_review_pass | none |
| F-024 | §8.2 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | design_review_pass | none |
| F-025 | §§7,13 | `python -m nox -s validation_module_registry_l0`; `python -m nox -s l0` | design_review_pass | none |
| F-026 | §§4.1,4.2,4.5,4.8 | `backend/tests/quantevolver/test_qe_active_dataset_profile.py`; `backend/tests/unified_engine/test_multi_alpha_command_generation.py` | implementation_review_pass | none |

## 13. DESIGN-COMPLIANCE-001 逐项审核

| 检查项 | 设计结论 | 直接理由 |
|---|---|---|
| 禁止简化交付 | PASS（设计） | profile、所有 QE 创建入口、双节点、UI/MCP、single/union/compare、覆盖缺口和历史兼容均纳入同一完整方案；26/26 acceptance items 已覆盖 |
| 禁止静默错误 | PASS（设计） | 启用后的配置、路径、hash、日期和覆盖异常均 typed fail closed；禁止旧路径/默认池/空集合回落 |
| 禁止改变业务逻辑 | PASS（设计） | 默认仍为 canonical stock-universe；显式 pool 只改变 universe；模型、因子、seed、TWAP、费用和 benchmark 语义不变 |
| 禁止私增门禁审批 | PASS（设计） | 无配置 DB、daemon、全量复扫、收益阈值或人工 signoff；只保留输入正确性和既有动作授权边界 |

## 14. Review History / 多轮审核记录

| revision | 审核重点 | 发现 | 修订 | 状态 |
|---|---|---|---|---|
| Draft-0 | 初稿 | F2 validator 不识别三个计划证据；默认日期入口清单不完整；原子切换缺少最小执行入口；一处日期笔误 | 修正证据路径；增加按业务行为分类的日期入口表；限定一个无后台服务的 validate/activate CLI；修正日期 | resolved |
| Review-1 | 单一权威与过度工程化 | profile path 若从多个环境变量推导会重建多源配置；纯因子库统计不应被 QE 回测默认值迁移顺带改变 | `AISTOCK_ACTIVE_DATASET_PROFILE_PATH` 作为唯一位置权威；以“是否执行 QE 回测”界定本阶段范围 | resolved |
| Review-2 | binding 生命周期与实验语义 | “创建或 materialize”存在双重解析歧义；按池过滤已有 prediction 不能冒充股票池专用训练 | 固定每条入口唯一 resolve-once 时点；将迁移性回测和专用训练拆成两种明确实验，并先低成本过滤诊断 | resolved |
| Review-3 | 数据交接与运行时边界 | “部署 sidecar”仍可能被误解为复制或修改整个 candidate；backend、frontend、MCP 和节点重启状态可能被合并 | 固定六个小文件和一个 coverage receipt 的版本化根；明确大组件零写入、三类激活分别报告、节点 API 默认无需重启 | resolved |
| Review-4 | 日期与指标口径 | cutoff、最新信号日、分钟执行终点和 h20/h40 outcome 可观测终点若共用一个字段，会再次把未成熟标签当成最新结果 | 增加由 release 日历和 label horizon 推导的 `outcome_observable_end`；最新信号继续保留，评价只使用共同成熟窗口 | resolved |
| Review-5 | F2 完整性与仓库门禁 | 复核必需章节、26 项设计矩阵、可执行证据、文档格式、ownership 和新增 guardrail | F2 validator PASS（26 items/26 rows/0 warnings）；`validation_module_registry_l0` 8 passed；`l0` 0 finding；`git diff --check` clean | pass |
| Review-6 | 最新主线语义复核 | 同步 `origin/main@983739db3` 后 BUG-1381 已让因子 analytics 接受 direct-v2 universe key，但日期常量、实验 binding 和指数 sidecar 消费仍未统一 | 保留纯因子统计的后续 adapter 边界；确认本设计只收敛真实 QE 训练/预测/回测入口，与 BUG-1381 无文本或语义冲突 | pass |
| Review-7 | 实现期最小化与双节点一致性 | 原设计要求每个节点维护 `stock_pool_root`，形成与 active profile 平行的第二部署面；初版实现尚未落地 separate-runs | 改为控制端校验 primitive、既有 workspace payload 携带单个 run sidecar、节点只登记 candidate root；增加复用 custom-evo 的比较展开器、group identity 和 UI/MCP 结构化入口 | resolved |
| Review-8 | mixed-release 内部旁路 | 内部直接调用 composer 时可能只继承活动日期却继续使用 legacy 数据根 | 活动 profile 启用后，无 run-scoped binding 的 composer/regenerate 调用 typed fail closed；legacy 日期只留在未激活兼容路径 | resolved |
| Review-9 | 公开输入边界与错误语义 | REST `custom_params` 可携带内部 binding；profile 输入错误可能退化成无结构 500 | 活动 profile 下拒绝 server-owned 字段；按请求错误与服务配置错误分别返回稳定 reason code | resolved |
| Review-10 | 已创建任务不漂移 | custom-evo 编辑/重跑可能在月更后重新读取当前 profile，且 editable response 可能泄露 binding | 编辑/重跑复用原 binding 且不读 profile；追加要求 generation/release/cutoff 一致；公开配置只显示语义 selection 与安全摘要 | resolved |
| Review-11 | 身份追溯账实一致 | 初版只持久化 generation/release/cutoff，缺设计要求的 profile digest 与 resolved timestamp | 将 digest 与 UTC resolved time 写入 server-owned summary，REST/UI/MCP 继续过滤 hash/path | resolved |
| Review-12 | coverage 合同最小化 | §5.4 把数据准备完整签收证据与 QE 高频消费投影写成同一个文件，和严格 parser 不一致 | 分为独立完整证据与 hash-pinned 最小 projection；精确定义四个顶层字段和 pool/gap 字段 | resolved |

当前文档随实现 PR 继续接受最终 HEAD 验证；在 PR 全部门禁完成前不得表述为源码可合入，在用户后续授权前不得表述为
活动 profile 已切换、后端/前端/MCP 已生效或实验已运行。
