# 核心指数 PIT 多股票池统一消费 F2 详细设计

> Feature ID：`core_index_pit_multi_pool_v1`
>
> 设计级别：F2（跨数据准备、QE/HMM、Selection、Paper、Advisory 的共享数据合同）
>
> 日期：2026-09-04
>
> 状态：设计完成，待按本设计分阶段实施
>
> 上位合同：`qe_pit_v2_dataset_upgrade_master_f2_design_20260813.md` 的 2026-09-01 最终目标修订、`unified_canonical_equity_pit_f2_design_20260812.md` 的唯一权威股票池规则
>
> 本设计不执行 DDL/DML、数据导出、候选改写、生产激活或进程控制。

## 1. Background / 背景

当前 2026-08-31 direct-v2 候选已经包含覆盖全市场的日线、分钟线、静态因子、指数上下文和 canonical equity PIT instruments。新增沪深300、中证500、中证1000、科创50、科创100等核心指数股票池，不需要重新导出这些大体量组件；缺口是指数成分的历史 PIT authority、共享解析方式和消费者可选配置。

现有 direct-v2 消费合同仍将选股范围固定为 `stock_universe`：

- `qe_validate_direct_v2_dataset.py` 与 `qe_dataset_contract.py` 只接受 `stock_universe`；
- `config_composer.py` 会拒绝 direct-v2 下其他股票池；
- `qe_build_frozen_risk_policy.py` 只接受 `all.txt` 或 `stock_universe.txt`；
- `stock_pool_sync.py` 已能读取多区间 instruments，但文件名合同仍偏向 `filtered_pool_*`。

生产数据库目前没有通用核心指数成分 PIT 表。`market.sw_index_member` 是申万行业分类数据，不能替代宽基/主题指数成分历史。Tushare `index_weight` 可以提供历史月度成分快照与权重，适合完整性交叉核对，但不单独证明指数调整的精确生效日；精确进出日期应来自中证指数、上交所、深证信息等指数编制机构的调整公告及附件。

本设计落实以下用户批准方向：

1. 基础成分 PIT 先一次性固化到数据库，再执行一次完整历史校验；
2. 以后月度更新只校验本次新增或变更的数据库行；
3. 导出只把数据库中已校验区间渲染为小型 instruments sidecar；
4. 导出阶段不再增加数据冻结、全历史哈希、全历史复扫、重复值级校验、资源准入或人工审批门禁；
5. 不重写日线、分钟线、H5、Parquet 或现有 `all.txt`；
6. 所有消费者默认行为保持不变，仅在显式选择新股票池时进入新路径。

## 2. Feature Card / 功能卡

| 项目 | 设计结论 |
|---|---|
| 用户问题 | 同一份全市场数据集无法按交易日准确选择一个或多个核心指数的历史成分股 |
| 用户结果 | QE、HMM、荐股、模拟盘等模块可以选择全市场、单指数或多个指数并集；重复股票只计算一次 |
| 数据正确性 | 原始指数成分 PIT 与现有 canonical equity PIT 相交；历史训练、回测和预测均按各自决策日解析 |
| 兼容性 | 未提供 `universe_selection` 时继续使用现有 `stock_universe`，现有实验、模型、策略包和运行配置不变 |
| 数据集影响 | 复用现有全市场大文件，只新增小型指数 instruments sidecar 或运行时组合 sidecar |
| 性能目标 | 单次解析只扫描所选指数的区间行；不展开全量“股票×交易日”矩阵，不触碰大体量行情/因子文件 |
| 数据源 | 指数编制机构公告/附件确定精确生效日；Tushare `index_weight` 做月度快照完整性交叉核对 |
| 本设计交付 | 数据合同、解析语义、最小模块改造、实施顺序、一次性与月度验证方案 |
| 非本 PR 交付 | 数据库表、历史数据回填、业务代码、真实导出、生产启用 |

## 3. Scope / 范围

### 3.1 包含

- 核心指数清单和统一 `pool_id`；
- 一张共享指数成分 PIT 数据表；
- 单指数、多个指数并集和现有全市场三种选择模式；
- 重复股票去重、PIT 区间合并、canonical equity PIT 相交规则；
- 数据库初始全量校验和后续月度增量更新流程；
- Qlib 小型 instruments sidecar 生成合同；
- QE、HMM、Selection、Paper、Advisory 的最小适配边界；
- 训练、验证、回测、预测和模拟盘之间的股票池一致性要求；
- 不完整历史数据与物理行情缺失的明确处理。

### 3.2 目标指数

优先级只决定数据回填与适配顺序，不创建不同技术架构。

| 优先级 | pool_id | 指数代码 | 用途 |
|---|---|---|---|
| P0 | `csi300` | `000300.SH` | 大盘核心 |
| P0 | `csi500` | `000905.SH` | 中盘核心 |
| P0 | `csi1000` | `000852.SH` | 小盘核心 |
| P0 | `star50` | `000688.SH` | 科创板大盘 |
| P0 | `star100` | `000698.SH` | 科创板中盘 |
| P1 | `sse50` | `000016.SH` | 沪市龙头 |
| P1 | `chinext` | `399006.SZ` | 创业板核心 |
| P1 | `csi_a500` | `000510.SH` | 行业均衡核心宽基 |
| P1 | `csi2000` | `932000.CSI` | 微盘/小微盘宽基 |
| P2 | `csi800` | `000906.SH` | 沪深300与中证500综合宽基 |
| P2 | `szse_component` | `399001.SZ` | 深市核心 |
| P2 | `sse180` | `000010.SH` | 沪市中大盘 |
| P2 | `szse100` | `399330.SZ` | 深市大盘 |
| P2 | `chinext50` | `399673.SZ` | 创业板龙头 |
| P2 | `csi_all_share` | `000985.CSI` | 全市场比较基准 |

首轮只需完成 P0；P1/P2 复用同一表、同一解析器和同一 sidecar 生成器，不另建框架。

## 4. Non-goals / 非目标与硬边界

本设计明确不做：

- 不重新导出全量日线、分钟线、H5、Parquet 或指数上下文；
- 不修改已完成的 2026-08-31 候选大文件；
- 不把指数价格序列与指数成分 PIT 混成同一 authority；
- 不用当前成分回填历史，不用公告日猜测生效日，不用月末快照冒充精确调整日；
- 不把 `market.sw_index_member` 改造成通用指数成分表；
- 不按日展开并保存“指数×股票×交易日”宽表；
- 不为每个消费者复制成分表、并集逻辑或去重逻辑；
- 不引入新的微服务、队列、CAS、registry、pointer、审批工作流或发布状态机；
- 不新增 source-freeze、artifact-freeze、全目录哈希、全历史哈希、发布前复扫或重复验收；
- 不新增 CPU、内存、commit headroom、磁盘预测、swap 或并发资源门禁；
- 不新增必须先跑小股票样本才能月更的流程门禁；
- 不改变模型算法、因子、标签、seed、持仓、交易费用、涨跌停、停牌或 benchmark 语义；
- 不让指数本身进入股票选股 universe；benchmark 与选股池始终独立；
- 不自动激活新股票池，不迁移现有实验、模型或策略包。

## 5. Architecture / 架构

### 5.1 单一数据流

```text
官方调整公告/附件 ──┐
                     ├─> 指数成分入库器 ─> market.core_index_membership_pit
Tushare index_weight ┘                         │
                                              ├─> 共享 resolve_universe()
market.stock_universe_pit_spans ──────────────┘        │
                                                       ├─> QE/HMM instruments sidecar
                                                       └─> Selection/Paper/Advisory symbol list
```

只有数据库 PIT 行是成分 authority。sidecar 是数据库查询结果的轻量表示，不是第二份权威，也不拥有独立生命周期。

### 5.2 两层语义

1. `raw membership`：指数编制机构公布的真实成分进入/退出区间；
2. `tradable universe`：`raw membership` 与现有 `aistock_equity_pit_canonical_v2` 区间相交后的股票池。

所有交易、训练和选股消费者只使用第二层。第一层保留指数真实成分事实，不因 AIstock 的 252 交易日、ST、退市等规则而被改写。

### 5.3 共享组件边界

首轮实现只新增或扩展三类共享能力：

- 数据库 migration 与一次性/增量入库脚本；
- 位于现有数据服务边界内的 `resolve_universe(selection, trade_date_or_window)`；
- 位于现有 dataset/QE 工具边界内的 instruments sidecar 渲染器。

各业务模块只负责把自身配置转换成统一 `UniverseSelection` 并消费解析结果；不得自行查询 Tushare、解析公告或实现并集。

## 6. Contracts / 契约

### 6.1 数据库表

建议新增唯一业务表 `market.core_index_membership_pit`：

| 字段 | 类型/语义 |
|---|---|
| `pool_id` | 稳定短名，例如 `csi300` |
| `index_code` | 指数代码，例如 `000300.SH` |
| `ts_code` | canonical A 股代码 |
| `effective_from` | 成分关系生效交易日，含当日 |
| `effective_to_exclusive` | 失效交易日，不含当日；当前成分为 NULL |
| `source_provider` | `CSI`、`SSE` 或 `CNINDEX`；必须是确定生效日的指数编制机构 |
| `source_reference` | 官方公告/附件的短定位符；不进入运行时 sidecar |
| `updated_at` | 数据库写入时间，不参与 PIT 生效判断 |

约束保持最小：

- 主键：`(pool_id, ts_code, effective_from)`；
- `effective_to_exclusive IS NULL OR effective_to_exclusive > effective_from`；
- 同一 `(pool_id, ts_code)` 区间不得重叠；
- writer 与完整校验按 §3.2 固定目录确保同一 `pool_id` 只映射一个 `index_code`；
- `updated_at`、抓取时间或 Tushare 月末日期均不得覆盖官方 `effective_from`。

`index_weight` 的月度权重不写入成员区间表：权重随月变化，与成分进入/退出区间不是同一事实，本功能也不做加权组合。Tushare 快照只在入库校验时使用。不新增版本 registry、pointer、事件 ledger、快照权重表、每日展开表或每消费者映射表。数据修订通过普通受控 upsert 和数据库审计字段完成。

### 6.2 来源优先级

| 层级 | 来源 | 用途 | 不允许的替代 |
|---|---|---|---|
| L1 | 中证指数、上交所、深证信息等编制机构调整公告及附件 | 进入/退出名单与精确生效日 | 不能用 Tushare 月末日期替代 |
| L2 | Tushare `index_weight` | 月度成分数量、名单与权重交叉核对；公告缺失定位 | 不能单独证明日内/精确生效边界 |
| L3 | Tushare `idx_anns` | 发现公告链接 | 不能作为成分名单 authority |

若某历史区间缺少精确生效依据，该 `pool_id` 的 `ready_from` 从首个完整可证明日期开始。解析更早日期返回 `membership_history_unavailable`；这属于输入不可用结果，不是新增工作流或人工审批门禁。

### 6.3 UniverseSelection

```yaml
universe_selection:
  mode: index_union
  pool_ids:
    - csi300
    - csi500
    - star50
  benchmark_code: 000906.SH
```

支持三种模式：

| mode | pool_ids | 结果 |
|---|---|---|
| `stock_universe` | 省略 | 完全保持现有 canonical 全市场股票池 |
| `single_index` | 恰好一个 | 该指数成分 PIT 与 canonical equity PIT 的交集 |
| `index_union` | 一个或多个 | 所选指数成分 PIT 并集去重后与 canonical equity PIT 的交集 |

兼容规则：

- 旧配置没有 `universe_selection` 时等价于 `mode=stock_universe`；
- 现有 `stock_pool=all/stock_universe` 继续按原路径工作；
- 新字段只在显式设置时生效，不自动迁移历史配置；
- `benchmark_code` 独立于 `pool_ids`，不得取第一个指数作为隐式 benchmark。

### 6.4 多指数并集与重复股票

给定交易日 `d` 和去重后的指数集合 `P`：

```text
index_members(d) = UNION(members(pool_id, d) for pool_id in sorted(unique(P)))
resolved(d) = index_members(d) INTERSECT canonical_equity_pit(d)
```

- `pool_ids` 先去空、校验、排序、去重，因此输入顺序不改变结果；
- 同一股票同时属于多个指数时，输出只有一个 canonical symbol；
- 可在诊断结果中返回 `source_pool_ids`，但不得生成重复预测、重复订单或叠加权重；
- 本功能是布尔股票池，不定义跨指数权重合成；
- 对区间输出，同一股票在不同所选指数中的区间先做集合并集：重叠或交易日相邻区间合并，有真实空档则保留多段；
- 再与 canonical equity PIT 多区间相交，得到最终 instruments 区间。

### 6.5 解析器输入输出

建议共享入口：

```python
resolve_universe(
    selection: UniverseSelection,
    start_date: date,
    end_date: date,
) -> ResolvedUniverse
```

`ResolvedUniverse` 最少包含：

- `mode`；
- 排序去重后的 `pool_ids`；
- `benchmark_code`；
- `membership_revision`（数据库提交批次或最大 `updated_at` 的稳定标识，不是全量内容哈希）；
- `intervals`：`symbol/start/end_inclusive`；
- `source_pool_ids_by_symbol`（可选诊断字段）；
- `unavailable_reason`（仅在无法解析时出现）。

稳定 unavailable reason：

- `unknown_pool_id`；
- `membership_history_unavailable`；
- `membership_interval_invalid`；
- `canonical_equity_pit_unavailable`。

不得返回默认行业、默认指数、空集合伪成功或退回当前成分。

### 6.6 Qlib instruments sidecar

数据库使用半开区间；Qlib instruments 使用含首尾日期。渲染时将 `effective_to_exclusive` 转换为前一交易日，并在与 canonical equity PIT 相交后输出：

```text
SH600000  2024-01-02  2024-12-13
SH600000  2025-06-16  2026-08-31
```

文件策略：

- 基础文件：`index_pool__csi300.txt`、`index_pool__csi500.txt` 等；
- 多指数并集：按运行请求生成一个小型 `filtered_pool__index_union__*.txt`；
- 文件只包含 `symbol/start/end`，不复制来源、哈希或数据库审计字段；
- sidecar 可放在候选的 `instruments/` 或现有 `stock_pools`/run workspace，由现有 stock pool sync 安装；
- 不修改 `stock_universe.txt`、`benchmark.txt` 或现有 `all.txt`；
- 不把 `000300.SH` 等指数代码写入选股 sidecar；benchmark 继续由独立 `benchmark.txt`/provider 读取。

这里“不修改数据文件”精确指不改写价格、成交量、复权、停牌、因子、指数序列等 Bin/H5/Parquet 内容。sidecar 是新增的小型选择清单；优先写入现有 `stock_pools`/run workspace。若现有 Qlib provider 只能从 `instruments/` 读取，stock pool sync 只安装该新增清单，不改写来源候选的任何既有文件。

sidecar 生成是纯 SQL 查询、区间集合运算和文本写入。它不再次下载来源、不再次验证全历史、不扫描日线/分钟/H5/Parquet、不计算目录或大文件哈希、不创建 freeze/attestation/signoff。

### 6.7 训练与预测一致性

- 专用股票池模型：训练、验证、回测和预测必须声明同一 `UniverseSelection`；
- 通用全市场模型：可在模型元数据中声明允许的预测 pool 模式，经过业务模块自身既有模型验收后用于子股票池；
- 不允许把“在全市场训练”自动解释成“任何指数池均已验证”；
- HMM 若只消费指数时间序列，无需指数成分 PIT；若消费横截面、行业宽度或成分聚合，则必须使用本解析器；
- 同一物理全市场数据集可服务所有股票池，差异只在样本/预测 universe，不需要复制数据集。

模型或运行最少记录：`mode`、排序去重后的 `pool_ids`、`membership_revision`、`dataset_release`、`cutoff`、`benchmark_code`。这些是普通运行参数，不新增发布 gate 或内容冻结。

## 7. Database Materialization and One-time Validation / 入库与一次完整校验

### 7.1 初始历史固化

P0 五个指数作为一个初始批次按以下顺序执行：

1. 从官方历史调整公告/附件整理进入、退出名单和生效日；
2. 用 Tushare `index_weight` 月度快照核对月末名单和成分数量；权重不写入成员 PIT；
3. 标准化证券代码，形成半开区间；
4. 在 DEV 执行 migration、导入和 readback；
5. DEV 结果通过后，按用户单独授权在生产执行同一 migration/import/readback；
6. 全部 P0 数据固化完成后执行一次完整历史校验并保存紧凑汇总。

### 7.2 唯一完整历史校验

完整校验只针对数据库基础数据和现有物理数据覆盖。遵循 DEV-first 标准，在 DEV 首次整体固化后执行一次；获授权写入生产后，在生产首次整体固化后再执行一次。它不按指数、组件或消费者重复执行，也不复制到后续导出阶段：

- 表结构、主键、代码格式、日期合法性；
- 同一指数/股票区间无重叠，进入/退出边界与官方生效日一致；
- 官方每次调整前后成分数量与增删名单闭合；
- 可获得的 Tushare 月度快照与相应月末 PIT 名单一致；
- P0 各指数在声明可用窗口内均有明确成员集合；
- 与 canonical equity PIT 相交后，结果只包含当日合格 A 股；
- 单指数与任意 P0 组合满足 order-invariant、重复股票一次、多区间正确；
- 2026-08-31 数据集的日线、分钟线物理 feature 目录能覆盖解析出的股票区间；
- 已知缺口必须形成精确小名单并走定向补齐，不能通过丢股票、缩日期、前填或全量重导掩盖。

此前抽样已发现至少以下历史 feature 缺口：日线 `600837.SH`、`601989.SH`、`000627.SZ`、`300379.SZ`、`601028.SH`，分钟线 `600837.SH`、`601777.SH`。实施时先重新做一次精确覆盖清单；若仍缺失，只补这些证券的缺失组件和日期，不重导全市场。

完整校验输出只保留：各 pool 覆盖起止、区间行数、代表日期成分数、重叠/非法行数、Tushare 差异数、物理数据缺失小名单、最终结论。禁止生成逐行成功日志、全历史内容哈希或大规模校验副本。

### 7.3 后续月度更新

初始校验完成后，每月只执行：

1. 查询上月末以来的新公告/附件及 Tushare 月度快照；
2. 在单个数据库事务中 upsert 新增/变更区间；
3. 仅对本次新增/变更行做代码、日期、重复、区间重叠和调整名单 readback；
4. 提交后按所需 pool 重新生成小型 sidecar；
5. 现有数据集月更继续按其正常导出流程运行。

不重复完整历史校验，不扫描未变化历史数据，不建立 source freeze，不计算全历史或全目录哈希，不增加人工批准点。没有新调整时为正常 no-change，不创建空跑校验任务。

## 8. Minimal Consumer Changes / 消费者最小修改

### 8.1 共享数据层

新增一次 `UniverseSelection`、`ResolvedUniverse` 和 resolver。数据库查询、PIT 区间运算、并集去重及 unavailable reason 只在这里实现。

### 8.2 Dataset release / Qlib 数据准备

最小改动：

- 增加从共享 resolver 写小型 instruments sidecar 的步骤；
- 月度候选仅在指数成分发生变化时重写相关 sidecar；
- 不使 sidecar 变化触发 daily/minute/H5/static/index_context 全量失效；
- 对已知物理缺口使用现有增量导出能力做精确证券/日期补齐；
- 不恢复 source-freeze、全历史 hash、发布前复扫或资源门禁。

### 8.3 QE

最小改动：

- direct-v2 合同从“只允许 `stock_universe`”扩展为“允许已解析的 instruments sidecar”；
- composer 接受 `universe_selection`，生成现有 `market`/instruments 配置；
- 复用 `stock_pool_sync.py` 的多区间解析和节点安装，仅扩展安全文件名；
- `qe_build_frozen_risk_policy.py` 接受 resolver 生成的 instruments 文件；
- 现有 `stock_universe` 默认和 benchmark 合同不变。

### 8.4 HMM

- 纯指数序列 HMM：零修改；
- 横截面/成分聚合 HMM：只增加可选 `universe_selection` adapter，并使用同一 sidecar/resolver；
- 不修改 HMM state、feature、训练参数、阈值或模型选择。

### 8.5 Selection / Advisory

- 在候选生成入口增加可选 `universe_selection`；
- 显式选择时从共享 resolver 取得当日 symbol set；
- 后续因子、打分、排名、容量和风险逻辑不变；
- 未设置时继续使用当前全市场 universe。

### 8.6 Paper / 模拟盘

- 只读取策略包/运行配置已经声明的 `UniverseSelection`；
- 用决策日 resolver 结果限制候选输入；
- 持仓卖出管理不因股票后来退出指数而被删除或失管；
- 订单去重仍按 canonical symbol，指数重叠不会生成重复订单；
- broker、行情、停牌、涨跌停、T+1 和执行算法不变。

## 9. Implementation Plan / 实施方案

实施压缩为三个交付批次，不为数据表、resolver、sidecar 分设串行 PR。模块可按所有权独立开发，不要求冻结其他业务窗口；只有同一文件发生真实冲突时才协调。

### Batch 1：单一数据准备实现 PR（数据准备窗口）

一次完成：

- `backend/migrations/core_index_membership_pit_20260904.sql`：唯一成员 PIT 表；
- `scripts/prepare_core_index_membership_pit.py`：官方成分变更与 Tushare 快照核对、DEV/生产 plan/apply/readback；
- `backend/services/core_index_membership.py`：`UniverseSelection`、repository 和共享 resolver；
- `backend/services/dataset_release/index_pool_sidecar.py`：primitive/union sidecar 渲染；
- `stock_pool_sync.py` 的最小安全文件名扩展；
- 对应数据库、resolver、sidecar 和默认行为测试。

该 PR 不回填真实数据、不写生产、不触发导出。数据准备能力以一个 PR 交付，避免人为拆成多个前置阶段。

完成条件：代码在 DEV fixture/数据库合同上通过，单指数/多指数解析与 sidecar 可用，现有全市场默认行为不变。

### Batch 2：数据库固化、一次完整校验与精确补缺（数据准备窗口）

1. DEV migration/import/readback，并在 DEV 首次整体固化后运行一次完整历史校验；
2. 获得精确授权后执行 production migration/import/readback，并在生产首次整体固化后运行一次完整历史校验；
3. 输出真实物理行情缺失小名单，只对精确证券/日期做增量补齐；
4. 生成 P0 primitive sidecar 和一个多指数 union sidecar；
5. 不重导全市场，不修改现有 `all.txt`、`stock_universe.txt`、`benchmark.txt` 或大组件。

完成条件：同一份现有数据集可通过 sidecar 选择全市场、单指数和多指数并集；大组件文件数量、大小和 mtime 不因本批次变化。

### Batch 3：按需消费者薄适配（各模块所有者）

- QE 首先扩展 direct-v2 pool contract/composer/risk-policy 文件选择；
- HMM 只有横截面/成分聚合入口需要 adapter，纯指数序列路径不修改；
- Selection/Advisory/Paper 仅在业务需要启用新池时增加可选配置；
- 同一所有者且无冲突的消费者适配可以合并为一个 PR，不强制逐模块拆分；
- 每个适配只增加配置转换、共享 resolver 调用和旧默认回归，不修改模型、策略或执行语义。

完成条件：显式新配置有效，旧配置输出不变。未选择新池的模块无需等待 Batch 3，也不需要修改。

真实启用时，QE 只做一次短 smoke；HMM 只验证实际使用横截面成分的入口；Selection/Paper/Advisory 只做无 broker side effect smoke。是否切换任何生产配置由用户单独决定。

## 10. Verification Plan / 验证方案

### 10.1 设计 PR

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/core_index_pit_multi_pool_f2_detailed_design_20260904.md --tier F2`
- `python scripts/ci_change_classifier.py --base origin/main --head HEAD --format summary`
- `python -m nox -s validation_module_registry_l0`
- `python -m nox -s l0`
- `git diff --check`

### 10.2 Phase A/B 数据测试

- `backend/tests/core_index_membership/test_membership_repository.py`：约束、半开区间、增量 upsert/readback；
- `backend/tests/core_index_membership/test_universe_resolver.py`：单指数、并集、重复股票、多区间、order-invariant、canonical PIT 相交；
- `backend/tests/scripts/test_prepare_core_index_membership_pit.py`：官方生效日与 Tushare 快照分工；
- DEV/生产完整校验命令只在首次固化各执行一次；生产 DDL/DML 分别保留授权和 readback 状态。

### 10.3 Phase C 数据集测试

- `backend/tests/dataset_release/test_index_pool_sidecar.py`：Qlib 日期转换、benchmark 隔离、原 instruments 不变；
- `backend/tests/quantevolver/test_stock_pool_sync.py`：新文件名、多区间和远端安装路径；
- temp fixture 证明 primitive 与 union sidecar；
- 对真实候选只核对 sidecar 可读和大组件前后文件统计不变，不做第二轮历史数据校验。

### 10.4 Phase D 消费者测试

- QE：旧 `stock_universe` 回归、single/union 配置、benchmark 独立；
- HMM：纯指数模式零变化，横截面模式按相同 selection；
- Selection/Advisory：旧默认结果相同，新 pool 仅改变输入 symbol set；
- Paper：重复股票只生成一次候选/订单，退出指数不删除存量持仓管理；
- 所有测试使用小 fixture，真实全量正确性已由数据库唯一完整校验负责。

## 11. Rollout and Rollback / 发布与回滚

### 11.1 发布

1. 先合入设计；
2. Batch 1 一次交付数据准备源码；
3. Batch 2 先 DEV、后按用户分别授权的生产 DDL/DML 固化数据并生成 sidecar；
4. Batch 3 各模块按实际需求适配，不切换任何消费者默认值；
5. 真实启用只由显式配置选择新股票池。

### 11.2 回滚

- 业务回滚只需删除/撤销新配置字段，消费者自然回到 `stock_universe`；
- sidecar 未被选择时不影响任何现有运行；
- 数据库成分历史可保留只读，不要求删除；
- migration rollback 只在尚未被任何消费者使用且用户明确授权时执行；
- 不通过覆盖旧数据集、重导大组件或修改历史实验实现回滚。

## 12. Production Gates / 生产边界（不新增门禁）

本设计不创建新的数据发布门禁。只有 AIstock 已有动作授权边界继续有效：

| 动作 | 本设计 PR | 后续要求 |
|---|---|---|
| DEV DDL/DML | noop | Batch 2 按既有 DEV-first 流程 |
| production DDL | noop | 必须由用户精确授权目标 migration |
| production DML | noop | DEV 成功后由用户精确授权目标表与批次 |
| 数据集导出/sidecar 写入 | noop | Batch 2；只写 sidecar/任务目录，不覆盖生产 |
| production activation | noop | 用户单独授权 |
| backend restart | noop | 如未来 runtime 代码需要，由用户执行 |
| worker/scheduler restart | noop | 仅实现 PR 的 runtime contract 明确要求时处理 |
| cleanup/delete | noop | 精确目标、单独授权 |

数据库约束、解析器的非法输入错误和既有运行异常不是新增审批门禁；它们只防止返回错误结果。不得把完整历史校验、抽样验证、资源利用率、哈希、signoff 或小样本实验设计成每月导出的前置条件。

## 13. Risks and Failure Modes / 风险与失败模式

| 风险 | 处理 | 禁止做法 |
|---|---|---|
| Tushare 月末快照不能证明精确生效日 | 以官方公告/附件为 L1；缺段显式 unavailable | 猜调整日、用月末日期替代 |
| 多指数含重复股票 | resolver 布尔并集并按 symbol 去重 | 重复训练样本、重复预测或订单 |
| 指数成员不满足 canonical PIT | 两层分离，交易 universe 做区间相交 | 修改原始指数成分历史 |
| 历史成员缺行情 feature | 输出精确小名单并定向补齐 | 丢股票、缩日期、前填、全量重导 |
| 新 sidecar 误使大组件失效 | dependency 只指向 instruments sidecar | 重建 Bin/H5/Parquet |
| 指数被当成选股证券 | benchmark 与 stock pool 文件隔离 | 把指数写入选股 sidecar |
| 各模块实现不同并集逻辑 | 所有模块复用共享 resolver | 各自 SQL、各自 Tushare 查询 |
| 新配置影响历史运行 | 默认仍为 `stock_universe`，新模式显式 opt-in | 自动迁移旧实验或策略包 |
| 月更再次过度工程化 | 数据库增量校验一次，导出纯渲染 | freeze/hash/rescan/资源门禁/重复 smoke |

## 14. Design Acceptance Index / 设计验收索引

| 编号 | 设计要求 |
|---|---|
| F-001 | 现有全市场数据集可复用，新增股票池不触发全量数据重导 |
| F-002 | 原始指数成分 PIT 与 canonical equity PIT 分层，交易 universe 使用两者交集 |
| F-003 | P0/P1/P2 指数使用同一表、同一 resolver 和同一 sidecar 合同 |
| F-004 | 官方公告/附件确定精确进出日期，Tushare `index_weight` 只做快照交叉核对 |
| F-005 | 支持 `stock_universe`、`single_index`、`index_union` 三种模式 |
| F-006 | 多指数输入排序去重且 order-invariant，重叠股票只出现一次 |
| F-007 | benchmark 独立配置，指数代码不进入选股 universe |
| F-008 | 初始数据入库后只执行一次完整历史校验，输出保持紧凑 |
| F-009 | 月更只校验新增/变更数据库行，导出不重复校验历史 |
| F-010 | 不新增 freeze、全历史/全目录 hash、复扫、资源门禁或人工审批点 |
| F-011 | sidecar 只含 symbol/start/end，不修改现有 all/stock_universe/benchmark 文件 |
| F-012 | 已知物理数据缺口只做精确证券/日期增量补齐，不做全量重导 |
| F-013 | QE/HMM/Selection/Paper/Advisory 使用薄 adapter，不复制 authority 或并集逻辑 |
| F-014 | 未显式配置时所有消费者继续现有 `stock_universe` 行为 |
| F-015 | 专用模型的训练、验证、回测和预测绑定同一股票池定义 |
| F-016 | 纯指数时间序列 HMM 不因本功能产生代码变化 |
| F-017 | Paper 持仓生命周期与指数退出解耦，重叠指数不产生重复订单 |
| F-018 | 数据缺段返回 typed unavailable，不用当前成分、空集合或默认股票池静默回退 |
| F-019 | 生产 DDL/DML、activation、restart、cleanup 保持既有独立授权边界 |
| F-020 | 设计和后续实现逐项满足 DESIGN-COMPLIANCE-001 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §§1,6.6,8.2 | `backend/tests/dataset_release/test_index_pool_sidecar.py` | design_review_pass | none |
| F-002 | §§5.2,6.4 | `backend/tests/core_index_membership/test_universe_resolver.py` | design_review_pass | none |
| F-003 | §§3.2,5.3 | `backend/tests/core_index_membership/test_universe_resolver.py` | design_review_pass | none |
| F-004 | §§1,6.2,7.1 | `backend/tests/scripts/test_prepare_core_index_membership_pit.py` | design_review_pass | none |
| F-005 | §6.3 | `backend/tests/core_index_membership/test_universe_resolver.py` | design_review_pass | none |
| F-006 | §6.4 | `backend/tests/core_index_membership/test_universe_resolver.py` | design_review_pass | none |
| F-007 | §§6.3,6.6 | `backend/tests/dataset_release/test_index_pool_sidecar.py` | design_review_pass | none |
| F-008 | §7.2 | `backend/tests/scripts/test_prepare_core_index_membership_pit.py` | design_review_pass | none |
| F-009 | §7.3 | `backend/tests/core_index_membership/test_membership_repository.py` | design_review_pass | none |
| F-010 | §§1,4,7.3,12 | `python -m nox -s l0` | design_review_pass | none |
| F-011 | §6.6 | `backend/tests/dataset_release/test_index_pool_sidecar.py` | design_review_pass | none |
| F-012 | §§7.2,8.2 | `backend/tests/dataset_release/test_index_pool_sidecar.py` | design_review_pass | none |
| F-013 | §§5.3,8 | `python -m nox -s validation_module_registry_l0` | design_review_pass | none |
| F-014 | §§6.3,8 | `backend/tests/core_index_membership/test_universe_resolver.py` | design_review_pass | none |
| F-015 | §6.7 | `backend/tests/quantevolver/test_core_index_pool_binding.py` | design_review_pass | none |
| F-016 | §§6.7,8.4 | `backend/tests/hmm_data_source/test_core_index_pool_adapter.py` | design_review_pass | none |
| F-017 | §8.6 | `backend/tests/paper_trading/test_core_index_pool_adapter.py` | design_review_pass | none |
| F-018 | §§6.2,6.5 | `backend/tests/core_index_membership/test_universe_resolver.py` | design_review_pass | none |
| F-019 | §12 | `python -m nox -s l0` | design_review_pass | none |
| F-020 | §16 | `python -m nox -s validation_module_registry_l0` | design_review_pass | none |

## 16. DESIGN-COMPLIANCE-001 逐项审核

| 检查项 | 设计结论 | 直接证据 |
|---|---|---|
| 禁止简化/子集/占位交付 | PASS（设计） | P0 是交付顺序而非独立简化架构；P1/P2 明确复用同一 authority/resolver/sidecar；F-001～F-020 全覆盖 |
| 禁止静默错误 | PASS（设计） | §6.2/§6.5 对未知池、历史缺段、非法区间和 PIT 不可用返回稳定 reason，禁止当前成分或空集合回退 |
| 禁止改变业务逻辑 | PASS（设计） | §8 仅增加可选 universe adapter；模型、因子、benchmark、交易和默认 `stock_universe` 均不变 |
| 禁止私增门禁 | PASS（设计） | §§4、7.3、12 明确导出零冻结、零全历史 hash、零复扫、零资源/人工门禁；仅保留既有动作授权边界 |

## 17. Review Record / 多轮审核记录

| 轮次 | 审核重点 | 发现 | 修订结果 |
|---|---|---|---|
| Review-1 | 数据 authority、PIT 因果日期、多指数重复 | 初稿需明确月度快照不能代替官方生效日，且原始成分与可交易 PIT 不能混写 | 已在 §§5.2、6.2、6.4 分层并固定来源优先级和区间运算 |
| Review-2 | 过度工程化、性能和月更关键路径 | 初稿需防止沿用旧 source-freeze/hash/全历史复扫语义 | 已在 §§4、6.6、7.3、12 明确取消，并限制为一表、一 resolver、一 sidecar writer |
| Review-3 | 消费者兼容与业务漂移 | 初稿需保证旧配置、benchmark、持仓生命周期和纯指数 HMM 不受影响 | 已在 §§6.3、6.7、8 固定 opt-in、benchmark 独立及消费者最小改动 |
| Review-4 | DESIGN-COMPLIANCE-001 与可实施性 | 需区分设计通过和代码实现完成，避免把未来测试写成已实现 | Matrix 状态统一为 `design_review_pass`；实现、DDL/DML、导出和 activation 明确留在后续阶段 |
| Review-5 | 仓库 F2 validator 首轮 | F-010/F-020 的证据命令不是 validator 可识别格式 | 改为仓库可执行、可解析的 `nox -s l0` 与 `validation_module_registry_l0` 证据 |
| Review-6 | 文档格式与数据文件边界 | `git diff --check --no-index` 发现 5 处行尾空格；“不修改数据文件”可能被误读为禁止新增 sidecar | 清理行尾空格；明确大组件只读、sidecar 是独立选择清单且不得覆盖来源候选既有文件 |
| Review-7 | 数据范式与完整校验次数 | 月度权重若写入成员区间表会混淆两种事实；“每个指数执行”可能被误解为重复全量校验 | 删除 weight/snapshot 字段和快照表需求；P0 一批固化，每个环境首次整体固化后只做一次完整校验 |
| Review-8 | 公开来源职责复核 | Tushare 文档确认 `index_weight` 是月度数据，`idx_anns` 输出公告日期/标题/链接/来源/类型而非成分及生效日 | 成员表只接受能确定生效日的官方 provider；Tushare 不写成 authority 行，只用于名单核对与公告发现 |
| Review-9 | 交付颗粒度与关键路径 | 将 authority、生产固化、sidecar 拆成独立 PR 会延长首个可用结果 | 压缩为一个数据准备实现 PR、一次数据固化批次和按需消费者适配；允许同 owner 无冲突适配合并 |

## 18. References / 参考

- Tushare index_weight：<https://tushare.pro/document/2?doc_id=96>
- Tushare idx_anns：<https://tushare.pro/document/2?doc_id=460>
- 中证指数沪深300编制方案：<https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/000300_Index_Methodology_cn.pdf>
- 中证2000指数资料：<https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/932000factsheet.pdf>
- 中证A500编制方案：<https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/000510_Index_Methodology_cn.pdf>
- 上交所指数公告：<https://www.sse.com.cn/market/sseindex/diclosure/>
- 国证指数列表：<https://www.cnindex.com.cn/zh_indices/sese/index.html?act_menu=1&index_type=-1>
