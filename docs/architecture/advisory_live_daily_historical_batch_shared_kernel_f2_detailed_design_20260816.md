# Advisory 实盘单日与历史批量同核执行 F2 详细设计

> 日期：2026-08-16
> Feature tier：F2
> 任务级别：T3
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.1
> 关联 golden 设计：`docs/architecture/advisory_historical_fullstack_comparison_f2_design_20260815.md`
> 状态：`USER_APPROVED_DESIGN_READY_IMPLEMENTATION_DEFERRED_UNTIL_CURRENT_V6_GOLDEN_FREEZE`
> 适用范围：荐股实盘单日运行、Historical Range 历史研究回放和 StrategyPackage/Selection 执行资源复用
> 业务边界：无资金、无下单、不写 Paper/QMT/模拟盘；历史执行不修改正式 Program/binding

## 1. Background / 背景与问题

当前荐股历史回放为了复用真实 StrategyPackage 推理语义，逐交易日调用 live inference 形态：

- `backend/services/strategy_package/multi_alpha_live.py` 按日期、按 Alpha leg 执行父腿推理；
- 历史日期被写入 source/workspace cache namespace；
- `backend/services/strategy_package/live_inference.py` 的 package source 和 prepared workspace 在每个日期/leg 重置、复制并重新生成；
- 历史模式关闭 Selection data cache；
- `backend/services/advisory_historical_range/candidate_producer.py` 在一次约9分钟的候选计算前后，对同一 Program/day source catalog 做两次完整 revision 验证；
- A/B 使用相同 StrategyPackage 和 raw Alpha 输入，但当前各自重复支付 raw 推理成本。

已观测的当前44日 A/B 回放基线约为每个 Program/day 9.5分钟。持久工作区总量约117MB，并不是主要容量问题；主要成本是反复静态工作区准备、Parquet/HDF转换、因子临时I/O、全量 source verification 和重复 raw Alpha 推理。单个活跃 WSL Python 进程 RSS 约4.7GB，首批 A/B 并发曾出现瞬时资源失败，因此本设计先消除重复工作，不以提高进程并发作为首要手段。

业务需求同时包含两种运行形态：

1. 实盘/前向荐股每日只处理一个决策日，需要即时错误可见、Program隔离和正式发布语义。
2. 历史回放属于回测性质，应一次接受日期区间，批量读取和复用资源，并保持逐日可恢复证据。

问题不应通过另写一套“回测荐股算法”解决。目标是分离执行拓扑与业务语义：实盘单日和历史批量使用不同 executor，但共同调用唯一的逐日业务组合。

## 2. Scope / 范围

本设计覆盖：

1. 定义与执行拓扑无关的逐日业务输入、输出和语义 hash。
2. 保留 `LiveDailyExecutor` 每日单日形态。
3. 新增 `HistoricalBatchExecutor`，一次接受冻结日期计划并按 chunk 连续执行。
4. 复用现有 StrategyPackage 日信号、HMM、risk、tradability、Selection projector 和 `AdvisoryListTransitionEngine`，不复制算法。
5. 提供只暴露单一 decision cutoff 的 `AdvisoryPITAsOfViewV1`。
6. 以内容 identity 复用静态模型、因子代码、配置和 prepared workspace。
7. A/B raw-affecting identity 一致时共享不可变 raw Alpha day artifact。
8. 将 source validation 分为批次、chunk、逐日读取和异常回退四层。
9. 保留逐日 artifact、typed failure、checkpoint、heartbeat、exact retry 和审计状态。
10. 建立 single-day/batch 语义等价、未来毒化、缓存污染、恢复和性能验收。

## 3. Non-goals / 非目标

- 不修改荐股候选、HMM、风险、可交易性、Selection、名单生命周期、止盈止损、成本或 outcome 算法。
- 不把多个交易日聚合成一次共同决策；批量只表示资源与调度批量化。
- 不在当前 v6 回放运行中修改代码、工作区、identity 或已封存 request。
- 不把历史结果写入实盘 Program、active binding、正式 forward observation、Paper、模拟盘或QMT。
- 不新建通用回测引擎、通用调度平台、通用缓存服务、ModelOps、GC或历史归档平台。
- 不默认增加多进程/多GPU并发；并发必须在单worker优化与资源验收后另行评估。
- 不删除旧 batch、旧 artifact、旧 workspace 或失败记录。
- 不以性能目标为理由删减 PIT、source revision、typed failure、readback、exact retry 或名单顺序语义。
- 不把 H0 作为 P0/P1 模型训练、前向发布、bundle激活或当前 A/B/C 结果闭合的前置条件。

## 4. 核心设计原则与不可变语义

### 4.1 执行拓扑与业务语义分离

同一业务输入必须满足：

```text
business_semantic_result(single_day(D))
  == business_semantic_result(batch([D])[D])
```

完整 artifact 的 `batch_id`、`operation_id`、worker、chunk、开始/结束时间和性能 telemetry 可以不同；以下字段必须一致：

- package/manifest/code release/runtime semantics；
- decision/target date 和 cutoff policy；
- source revision refs 和实际读取视图 hash；
- raw/component score、normalized score、rank 和 tie-break；
- HMM coefficient、risk/tradability action 和 exclusion reason；
- Selection effective score/rank 和 stage trace；
- candidate outcome、TopK和typed no-candidate reason；
- previous list identity、`ENTER/HOLD/EXIT/WATCH` 和退出原因；
- model/challenger输入与输出身份；
- outcome成熟状态和统计口径。

### 4.2 共享业务内核不是新单体

“同核”表示两种 executor 组合同一组既有权威组件，不新建第二套巨型算法服务：

```text
StrategyPackageDaySignalKernel
  -> existing SelectionSignalPreparation
  -> existing Selection computation/providers
  -> existing CandidateProjector
  -> existing AdvisoryListTransitionEngine
  -> AdvisoryDaySemanticResultV1
```

允许新增的共享层只负责显式输入合同、组件编排、语义 hash 和资源 session。算法继续由现有模块拥有。

### 4.3 每日独立与顺序依赖同时保留

- raw Alpha、HMM coefficient和候选计算可按日期独立预计算。
- Advisory名单、持仓episode和依赖前一日状态的退出/替换必须按交易日顺序提交。
- 首个未完成日之后不得继续提交 list/episode transition；已独立算出的后续 raw artifact可以保留为未消费缓存。
- 恢复从最后一个成功提交的 day checkpoint继续，不重写更早日期。

## 5. Architecture / 架构

```text
                         +-------------------------+
                         | Shared semantic services|
                         | package signal          |
LiveDailyExecutor ------>| HMM/risk/tradability    |----> day semantic result
  one Program/day        | Selection/projector     |      + live publication
                         | list transition          |
                         +-------------------------+
                                      ^
                                      |
HistoricalBatchExecutor --------------+
  frozen date plan
  persistent worker
  chunked PIT source
  immutable workspace session
  day checkpoint/artifact
```

### 5.1 `LiveDailyExecutor`

- 输入一个 Program、active binding、decision date和正式 data readiness。
- 通过正式 realtime source创建单日 `AdvisoryPITAsOfViewV1`。
- 调用共享逐日业务组合。
- 使用现有 forward/review repository执行正式发布或typed unavailable。
- 不读取 historical batch状态或研究 artifact。

### 5.2 `HistoricalBatchExecutor`

- 输入 sealed request、ordered trade dates、Program arms、source catalog、chunk policy和artifact root。
- 默认 `chunk_size=5`、`worker_count=1`、Program级并发1。
- 在一个持久worker中复用静态workspace session并顺序处理chunk。
- 每日生成独立 day result、artifact、receipt和checkpoint。
- A/B共享满足身份条件的 raw artifact，overlay和名单状态独立。
- 不调用 live publisher，不解析 active production binding，不修改正式 forward状态。

### 5.3 `AdvisoryPITAsOfViewProvider`

- 拥有chunk级批量读取结果；业务内核不得获得原始batch frame引用。
- `for_decision_date(D)` 返回只读view，强制business date、available time、first-observed/source revision和universe边界。
- view保存查询合同、列集合、row count、canonical content hash、cutoff和source refs。
- 任一调用尝试读取view授权范围外的日期或字段时typed failure。

### 5.4 `ImmutableRuntimeWorkspaceSession`

- 把package asset materialization和prepared workspace从日期生命周期提升到内容identity生命周期。
- 静态目录只读；日级数据、临时结果和诊断写入独立sandbox。
- session关闭时只回收本次动态资源，不删除共享静态内容。
- identity冲突、内容hash不一致、静态区写入或模型代码漂移立即失败。

### 5.5 `RawAlphaDayArtifactStore`

- 复用现有CAS/artifact store，不新建通用存储平台。
- raw artifact是date级、不可变、可readback的StrategyPackage输出。
- A/B等多个consumer引用同一raw artifact，不复制payload。
- overlay及最终candidate artifact继续按Program/arm独立发布。

### 5.6 `AdvisorySemanticParityService`

- 从single-day和batch输出提取规范化业务字段。
- 排除运行信封字段后计算 `day_business_semantics_sha256`。
- 输出字段级差异、首个差异路径和父输入identity；禁止只比较candidate count。

## 6. Contracts / 契约

### 6.1 `AdvisoryDayExecutionContextV1`

```text
execution_mode = LIVE_DAILY | HISTORICAL_BATCH
program_id / research_program_id
package_id / manifest_sha256 / code_release_hash
decision_trade_date / target_trade_date
runtime_semantics_hash
source_catalog_ref/hash
cutoff_policy_hash
previous_day_state_ref/hash
artifact_root
```

`execution_mode`只能影响资源、发布和运行信封，不得进入候选分数、rank或名单业务判断。

### 6.2 `AdvisoryPITAsOfViewV1`

```text
decision_trade_date
decision_timestamp
max_business_date
availability_policy
source_revision_refs
query_contract_hash
columns_hash
row_count
content_hash
universe_identity_hash
```

对于有 `available_at` 的来源必须执行 `available_at <= decision_timestamp`。没有正式availability列的来源必须由既有first-observed/revision合同覆盖；既无availability也无可靠revision的来源不能进入fast path。

### 6.3 `AdvisoryRawAlphaDayArtifactV1`

raw identity至少包含：

```text
package_id + manifest_sha256 + code_release_hash
decision_trade_date + cutoff_policy_hash
alpha_mode + component roster/model/factor identities
normalization + terminal weights + raw top-k semantics
universe_identity_hash + raw source view hashes
raw-affecting runtime config hash
```

HMM、risk、tradability和最终Selection overlay配置不进入raw identity，前提是现有执行顺序证明这些字段不影响raw生产。字段归类由显式registry维护；未知字段默认视为raw-affecting，禁止乐观共享。

### 6.4 `AdvisoryDaySemanticResultV1`

包含逐日业务字段、父artifact/source refs、stage trace、名单动作和 `day_business_semantics_sha256`。它不包含worker、chunk、wall time或cache hit；这些位于execution receipt。

### 6.5 `AdvisoryHistoricalBatchExecutionV1`

```text
batch/request/run identities
ordered trade dates
chunk size and worker policy
workspace session identities
last successful candidate date
last committed list-transition date
per-day status/artifact/semantic hashes
typed failure and resume cursor
performance receipt ref/hash
```

### 6.6 `AdvisorySemanticParityReceiptV1`

```text
golden_identity
single_execution_identity
batch_execution_identity
compared_trade_dates
equal_day_count / different_day_count
field_diff_refs
operational_envelope_differences
status = PASS | FAIL
```

任一业务字段差异均为FAIL；只允许运行信封不同。

## 7. PIT批量读取与未来数据隔离

### 7.1 数据获取

每个chunk使用短生命周期、只读 `REPEATABLE READ` 事务批量读取所需日期/列，形成不可变chunk source buffer，然后立即关闭事务。不得为整个44日计算持有长事务。

chunk source buffer可以位于Arrow内存、受控mmap或任务临时列式文件；选择由容量基准决定，但必须：

- 只包含合同声明的列与日期范围；
- 保存canonical row ordering和content hash；
- 不向业务内核暴露底层buffer；
- chunk完成或失败后按任务生命周期释放；
- 不演化为跨任务通用缓存。

### 7.2 日视图能力隔离

日内核只能调用：

```python
pit_view = provider.for_decision_date(decision_trade_date)
```

view provider在返回前执行：

1. `business_date <= decision_trade_date`；
2. `available_at <= decision_timestamp`，或等价first-observed/revision合同；
3. 股票池、行业映射、ST、停牌、HMM和价格各自的PIT有效区间；
4. source member属于sealed request和当前Program/day；
5. row/column/content hash与chunk receipt一致。

### 7.3 未来毒化验收

对D日以后的价格、行业成员、ST、停牌、HMM observation、资金和修订信息分别注入变化：

- D日 `day_business_semantics_sha256`必须不变；
- D+1对应结果允许变化；
- 若D日变化，测试必须给出泄漏字段和访问路径并阻断合入。

## 8. Source validation 分层优化

当前“计算前后两次完整Program/day verification”改为以下分层，不取消revision保护：

### 8.1 批次 full seal

- 创建请求时对date plan、source requirements和全部成员执行现有完整解析/hash。
- 冻结catalog hash、query contract和Program/day member identities。
- 同一request exact retry复用该seal；输入变化创建新identity。

### 8.2 chunk开始校验

- 对本chunk会读取的source成员读取revision token、formal event identity或等价轻量watermark。
- 与sealed catalog不一致则chunk不开始。
- 无可靠token的mutable来源标记为`FULL_HASH_REQUIRED`。

### 8.3 逐日实际读取receipt

- 记录业务内核真正消费的source view refs/hash，而不是只证明catalog存在。
- receipt覆盖price/universe/component/HMM/risk/tradability/model feature等实际角色。
- 缺角色、空view或越界均typed failure，不能降级A或空成功。

### 8.4 chunk结束校验

- 再读revision token；未变化则不重复全量内容scan。
- token变化、无token来源、row/hash异常或读取期间发现漂移时执行full rehash。
- full rehash不一致时，该chunk所有尚未正式提交的day结果失败；已提交结果只有在引用不可变实际输入snapshot时可保留，否则标记source drift并阻断后续transition。

### 8.5 不可版本化来源

无法提供availability/revision且会发生历史修订的来源只有两种合法处理：

1. 在短事务中物化本chunk实际读取的最小列/行到不可变任务CAS并引用其hash；或
2. 保留逐日pre/post full hash慢路径。

禁止因为性能目标直接跳过校验。

## 9. 静态工作区与数据生命周期

### 9.1 静态workspace key

```text
sha256(
  package_id,
  manifest_sha256,
  code_release_hash,
  leg_id,
  model_asset_sha256,
  model_code_sha256,
  factor_code_set_sha256,
  alpha158_schema_sha256,
  runtime_semantics_hash,
  workspace_builder_contract_version,
  inference_runner_contract_hash,
  inference_backend_identity,
  dependency_environment_hash
)
```

交易日期不得进入静态key。任何日期相关文件不得写进静态目录。

### 9.2 日级sandbox

```text
batch_id / program_id / decision_date / attempt_id
```

只保存该日PIT view链接/投影、临时因子结果、模型输出和诊断。硬链接、只读mmap或loader映射优先于复制；不支持时显式copy并记录字节数。

### 9.3 HDF/因子I/O

- 取消把同一个static factor parquet反复转换为多个内容相同的HDF alias。
- 优先由loader维护logical name到一个canonical只读数据对象的映射；确需文件名兼容时使用经验证的只读hardlink。
- 因子函数临时目录只保存日级输出，不复制完整静态输入。
- 修改loader前必须以现有模型对golden日期验证分数和rank完全一致。

### 9.4 生命周期与容量

- workspace session由batch显式打开/关闭；异常退出由任务级lease/owner恢复清理其动态sandbox，不自动删除共享静态内容。
- 缓存必须有最大容量、LRU/TTL或任务生命周期策略和只读readback。
- 代码提交、workspace builder、生成runner或依赖环境任一identity变化均不得命中旧workspace；即使package manifest未变也必须新建内容identity。
- 任何清理命令属于独立用户授权；实现和测试不能广泛删除现有runtime root。

## 10. A/B raw Alpha共享

### 10.1 合法共享条件

A/B以下字段必须完全相同：

- package/manifest/code release；
- decision date、universe和raw source views；
- Alpha腿roster、模型、因子、normalization、weights和raw top-k；
- 所有raw-affecting runtime字段；
- workspace/inference backend语义。

满足后只执行一次raw Alpha，并把同一artifact ref传给A/B。

### 10.2 分叉点

```text
AdvisoryRawAlphaDayArtifactV1
  -> A overlay: HMM off, risk policy off, A Selection/list
  -> B overlay: frozen HMM + ST/risk + tradability, B Selection/list
```

B不得把overlay结果写回raw artifact；C也只读A的规定父候选，不改变A/B。

### 10.3 拒绝共享

以下任一情况产生不同raw identity：

- package/manifest/model/factor/weight变化；
- universe、cutoff或source view变化；
- raw top-k、normalization或component roster变化；
- config字段归类未知；
- raw artifact readback/hash不一致。

拒绝共享只导致分别计算，不得回退旧artifact或latest。

## 11. 执行流程

### 11.1 实盘单日

1. 解析一个ENABLED Program和active binding。
2. 确认decision/target时钟与当日数据ready状态。
3. 建立单日realtime PIT view。
4. 打开/复用一个内容identity workspace session。
5. 调用共享日业务组合。
6. 原子写正式review/list/forward observation或typed failure。
7. 关闭日sandbox；静态session按实盘进程生命周期管理。

### 11.2 历史批量

1. 读取sealed request/date plan/source catalog，确认当前v6 golden已冻结且本次为新code-release identity。
2. 计算Program/arm raw-sharing groups。
3. 打开单个持久worker与所需静态workspace sessions。
4. 以默认5日chunk批量读取PIT source并完成chunk开始校验。
5. 对chunk内日期按序生成日view；raw共享组每日期只计算一次raw artifact。
6. 各arm独立执行overlay/candidate并发布staged日artifact；名单转移只生成绑定previous-state hash的staged plan。
7. chunk结束执行revision token校验/必要full rehash；校验失败不提交正式day/list状态。
8. 校验通过后按Program/arm日期顺序，以现有日级事务提交candidate、list transition和checkpoint，发布性能receipt。
9. 继续下一chunk；失败时保留已提交日、verified staged artifacts和resume cursor。

## 12. 状态、失败与恢复

### 12.1 状态层级

```text
BATCH: PLANNED -> RUNNING -> COMPLETED | WAITING_INPUT | RETRYABLE_FAILED | TERMINAL_FAILED
CHUNK: PLANNED -> SOURCES_SEALED -> RUNNING -> RESULTS_STAGED -> VERIFIED
       -> COMMITTING -> COMMITTED | FAILED
DAY:   CLAIMED -> RAW_READY -> OVERLAY_READY -> RESULT_STAGED
       -> CANDIDATE_COMMITTED -> LIST_COMMITTED -> COMPLETED | typed failure
```

已有Historical Range正式状态是权威；若没有chunk数据库实体，chunk状态先保存在不可变任务artifact和batch heartbeat中，不为telemetry新增DDL。

chunk计算期间只允许发布不可变的staged artifact，不得把正式day/list状态标记为成功。chunk结束revision校验通过后，
再按日期以现有日级事务提交candidate/list/checkpoint。若提交到第N日发生进程失败，前N-1日保持已提交，
其余日期引用同一verified staged artifact继续exact resume。每个staged list-transition plan在提交时必须以
`previous_state_hash + row_version`执行CAS；不匹配则typed conflict，不得重新读取变化后的source或重算状态冒充同一chunk。

### 12.2 失败隔离

- raw计算失败：该raw-sharing group/day的consumer均不进入overlay；其它不共享group可继续预计算。
- A overlay失败：B若只依赖共享raw可完成candidate预计算，但A/B各自有状态list transition不得跨过自身失败日。
- B HMM/risk失败：A可继续；B保持typed failure，不降级A。
- source drift：停止当前chunk正式提交并执行full rehash/immutable snapshot判定；未验证staged artifact不得绑定为成功day。
- workspace corruption：关闭该session，保留诊断，基于相同identity重新materialize后hash必须一致；不一致为terminal conflict。
- list transition失败：候选artifact可保留，列表从最后成功checkpoint exact resume。

### 12.3 Exact retry

相同sealed request、code release、runtime semantics、source refs和previous state必须得到相同day business hash。不同输入不得覆盖原day；创建新run/revision并保留旧事实。

## 13. 性能、资源与可观测性

### 13.1 Stage telemetry

每个batch/chunk/day记录：

- source seal/token/read时间、查询次数、rows和bytes；
- package source materialization、workspace prepare和cache hit；
- 各Alpha leg raw inference、HMM/risk/tradability、Selection/projector；
- artifact publish/readback、list transition和checkpoint；
- wall/CPU time、RSS peak、read/write bytes、GPU util/memory；
- raw artifact reuse count和拒绝共享原因。

### 13.2 首版资源策略

```text
worker_count = 1
program_concurrency = 1
chunk_size = 5
candidate_prefetch = 1
```

chunk size可在同一设计语义内通过基准调整为1至10；worker并发提高属于后续独立性能决策，必须证明内存和DB资源安全。

### 13.3 实现验收目标

正确性是硬门禁；性能目标未达时不得声明H0完整：

- 同一静态workspace identity每batch materialize不超过1次；
- 同一A/B raw-sharing group每date raw inference不超过1次；
- 对具有可靠revision token且无source drift的来源，完整full source scan为批次seal 1次，日执行不再pre/post各scan一次；
  显式`FULL_HASH_REQUIRED`来源继续执行设计规定的安全慢路径并单独计数；
- 代表窗口临时实际写入字节较golden降低至少70%；
- A/B完整窗口wall time较当前golden降低至少50%，目标稳定区间为每交易日2至4分钟等价工作量；
- 单进程峰值RSS低于8GB且无持续增长、无PostgreSQL `/dev/shm`回归。

若硬件噪声导致wall time不可比，必须至少满足前三项结构性指标和I/O/RSS收据，并明确性能结论未完成，不能用估算冒充实测。

## 14. API / DB / UI / 运行时影响

### 14.1 API/CLI

- 实盘现有after-close/API行为不变。
- Historical Range现有create/status/resume接口保持兼容；batch execution policy作为冻结request/runtime字段显式保存。
- CLI增加或扩展显式参数时必须包含`chunk-size`、`worker-count`、`semantic-parity-golden`和`telemetry-output`，默认值来自本设计，不读取dynamic latest。

### 14.2 Database

- 首版不新增DDL。
- 继续使用现有batch/run/day attempt/artifact/list/episode表和正式repository事务。
- chunk和性能细节优先写artifact；只有真实查询需求证明artifact不足时，另行更新设计并执行DEV-first DDL流程。
- 每日业务写入保持一个日级原子事务，不把44日放进一个长事务。

### 14.3 UI

H0无新增UI，L3/L4为`noop`。已有历史状态页面/API只需继续显示正确日状态，不新增性能控制面板。

### 14.4 Runtime

- 源码合入不自动激活HistoricalBatchExecutor。
- 当前v6继续使用原code-release到结果冻结。
- 后续启用batch path、用户后端重启和runtime identity readback分别由用户确认/执行。

## 15. Implementation Plan / 实施方案

### H0-0：冻结golden与测量合同

- 完成当前v6 A/B/C、outcome和报告。
- 冻结代表日及完整44日业务字段、source refs、artifact hashes、阶段耗时/I/O/RSS。
- 产出semantic normalization schema，明确运行信封排除项。

### H0-1：抽取共享语义合同

目标位置：

- `backend/services/advisory_execution/models.py`
- `backend/services/advisory_execution/semantic_hash.py`
- `backend/services/advisory_execution/business_kernel.py`

先让现有single-day路径调用新编排但保持输出完全一致；未证明等价前不开发batch快路径。

### H0-2：静态workspace session

目标位置：

- `backend/services/strategy_package/runtime_workspace_session.py`
- 定向修改 `live_inference.py`、`multi_alpha_live.py` 和 `selection_artifact.py`

实现内容identity、只读workspace、日sandbox、cache lifecycle和I/O telemetry。

### H0-3：PIT batch source与分层validation

目标位置：

- `backend/services/advisory_execution/pit_view.py`
- `backend/services/advisory_historical_range/batch_source.py`
- 定向修改 `catalog_postgres.py` 和 `candidate_producer.py`

先实现batch size=1与现有pre/post full verification等价，再启用chunk token fast path。

### H0-4：raw Alpha artifact共享

目标位置：

- `backend/services/advisory_historical_range/raw_alpha_artifact.py`
- `backend/services/advisory_historical_range/candidate_producer.py`
- `backend/services/strategy_package/multi_alpha_live.py`

建立显式raw-affecting config registry、identity、CAS readback和A/B分叉。

### H0-5：HistoricalBatchExecutor与恢复

目标位置：

- `backend/services/advisory_historical_range/batch_executor.py`
- Historical Range runtime factory/application service的最小适配
- 现有长任务CLI的batch策略参数和状态readback

实现chunk、day checkpoint、ordered list transition、heartbeat和exact resume。

### H0-6：等价、失败注入与性能验收

- batch size=1；
- 代表日矩阵；
- 完整44日golden；
- 未来毒化、cache poison、revision drift、chunk中断；
- A/B共享正反例；
- stage/I/O/RSS性能报告。

任何阶段修改业务语义必须先更新父蓝图和本设计，创建新golden identity，并从R1审核重新开始。

## 16. Verification Plan / 验证方案

### 16.1 L0合同测试

- context/view/raw/day/batch/parity模型schema和canonical hash；
- raw-affecting未知字段fail closed；
- execution_mode不进入业务语义；
- 运行信封排除字段固定且不可扩散。

### 16.2 L1定向测试

- `backend/tests/advisory_execution/test_single_batch_semantic_parity.py`
- `backend/tests/advisory_execution/test_pit_asof_view.py`
- `backend/tests/advisory_execution/test_execution_boundaries.py`
- `backend/tests/strategy_package/test_runtime_workspace_session.py`
- `backend/tests/advisory_historical_range/test_raw_alpha_reuse.py`
- `backend/tests/advisory_historical_range/test_batch_source_validation.py`
- `backend/tests/advisory_historical_range/test_batch_recovery.py`
- `backend/tests/advisory_historical_range/test_batch_resource_policy.py`
- `backend/tests/advisory_historical_range/test_batch_telemetry.py`

### 16.3 L2真实DEV/golden验证

代表日必须覆盖：

- 5月、6月、7月各至少一天；
- ST hard action；
- 停牌；
- 行业映射缺失并由B显式exclude；
- HMM改变Top20集合；
- A/B raw相同、overlay不同；
- 无候选或typed data unavailable；
- chunk边界及失败恢复日。

最后比较完整44日逐日semantic hash、候选、名单和outcome。不得用3日canary代替完整窗口等价证据。

### 16.4 性能验证

- 当前golden、batch size=1、batch size=5在同一机器、相同数据库配置和相同输入identity下各运行。
- 至少报告median/p90 day time、完整wall time、source queries/scans、raw inference count、workspace materialization count、read/write bytes和RSS。
- 第一次稳定运行用于warming诊断；正式比较必须说明cache cold/warm状态，不能混用。

### 16.5 审核循环

每次设计或代码修订后从R1开始：

1. R1 需求/蓝图/实现映射：逐项核对F-141至F-150和父蓝图，不允许业务逻辑分叉。
2. R2 PIT/安全/反事实：未来毒化、revision drift、cache poison、raw-sharing反例、exact resume和生产边界。
3. R3 全量diff/合入：重复实现、死代码、scope、测试真实性、资源、DDL/runtime和DESIGN-COMPLIANCE-001。

最后一次修订后R1/R2/R3必须连续通过；任一轮产生修订则计数归零。

## 17. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-141 | 实盘单日与历史批量仅执行拓扑不同，共享唯一逐日业务语义；禁止第二套回测选股、HMM、risk或名单算法 |
| F-142 | 历史批量日内核只能读取绑定decision cutoff、availability和source revision的PIT AsOfDataView；未来数据毒化不得改变较早日期结果 |
| F-143 | 静态工作区按package/manifest/model/factor/runtime内容identity复用，日期动态数据和结果逐日隔离；缓存错误不得静默重建为成功 |
| F-144 | A/B只在raw-affecting identity完全相同时共享不可变raw Alpha artifact；HMM/risk/tradability在raw之后分叉，identity差异必须拒绝共享 |
| F-145 | source validation使用批次full seal、chunk revision token、逐日读取receipt和异常full rehash；不得用单纯日期过滤替代revision校验 |
| F-146 | 每个历史交易日独立artifact、typed failure和checkpoint；raw可预计算，顺序相关的list/episode transition在首个未完成日停止并exact resume |
| F-147 | 当前v6冻结为golden baseline；代表日和完整窗口以业务语义hash验证batch与single-day等价，运行信封字段单独比较 |
| F-148 | 首版使用单持久worker和默认5日chunk，先验证内存/I/O/恢复再评估并发；并发不是业务完成或性能验收的替代 |
| F-149 | H0不修改实盘binding、Program发布语义、策略包状态、Selection/Paper/QMT，也不在当前v6运行中热改code-release |
| F-150 | 性能receipt分解workspace/source/raw/overlay/publish耗时、I/O、RSS和cache hit；目标未达不得删减PIT、typed failure或业务逻辑 |

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-141 | `backend/services/advisory_execution/business_kernel.py`; live/historical executors | `backend/tests/advisory_execution/test_single_batch_semantic_parity.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-142 | `backend/services/advisory_execution/pit_view.py`; `batch_source.py` | `backend/tests/advisory_execution/test_pit_asof_view.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-143 | `backend/services/strategy_package/runtime_workspace_session.py` | `backend/tests/strategy_package/test_runtime_workspace_session.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-144 | `raw_alpha_artifact.py`; `candidate_producer.py` | `backend/tests/advisory_historical_range/test_raw_alpha_reuse.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-145 | `batch_source.py`; `catalog_postgres.py` | `backend/tests/advisory_historical_range/test_batch_source_validation.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-146 | `batch_executor.py`; existing day/list repositories | `backend/tests/advisory_historical_range/test_batch_recovery.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-147 | `semantic_hash.py`; parity service | `backend/tests/advisory_execution/test_single_batch_semantic_parity.py` (target path); artifact: current v6 frozen comparison receipt | APPROVED_BY_USER_DESIGN_READY | none |
| F-148 | `batch_executor.py` resource policy | `backend/tests/advisory_historical_range/test_batch_resource_policy.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-149 | live/historical boundary adapters | `backend/tests/advisory_execution/test_execution_boundaries.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |
| F-150 | execution telemetry artifact | `backend/tests/advisory_historical_range/test_batch_telemetry.py` (target path) | APPROVED_BY_USER_DESIGN_READY | none |

## 19. Production Gates

| 项目 | 设计状态 | 说明 |
|---|---|---|
| production DDL | `noop` | 首版复用现有表；性能/chunk telemetry写artifact |
| production DML | `noop_for_design` | 实现后历史研究仍只经现有repository写任务表；不写实盘binding |
| DEV validation | `required_for_implementation` | 真实golden和资源验证使用现有DEV数据库，不创建新测试库 |
| backend dependency | `noop` | 复用现有Python/Pandas/Arrow/HDF/LightGBM/psycopg2能力；缺依赖需另行证明与授权 |
| frontend dependency | `noop` | 无UI范围 |
| runtime activation | `pending_user_action_after_merge` | 合入不自动切换历史executor |
| backend restart | `pending_user_action_after_merge` | 由用户确认并执行；本设计和当前v6不控制进程 |
| current v6 | `frozen_untouched` | 继续按原code-release完成并冻结golden |
| cleanup | `noop` | 不删除旧batch、artifact、workspace或分支 |

## 20. Rollout / Rollback / 发布与回滚

### 20.1 Rollout

1. 先完成并冻结当前v6 A/B/C、outcome、统计和性能golden。
2. H0在独立后续worktree/branch实施；不得复用正在变化的当前物理工作树作为第二写者。
3. 先让现有single-day路径经共享合同运行，证明零语义变化。
4. 启用batch size=1，完成代表日和失败注入。
5. 启用chunk size=5、workspace复用和raw共享，完成完整44日等价与性能验证。
6. R1/R2/R3、F2 validator、PR/CI全部通过后停止在`MERGE_READY_AWAITING_USER`。
7. 用户确认合入；runtime切换与后端重启继续作为独立用户动作。

### 20.2 Rollback

- 保留 `LiveDailyExecutor` 和现有历史逐日executor作为明确切换路径，禁止silent fallback。
- batch path失败时停止新batch提交；不影响实盘单日执行。
- 已完成batch day artifact/checkpoint保持审计可读，不删除、不改写。
- 回滚源码后新请求使用旧executor；旧batch按其code-release继续readback，不跨版本resume。
- workspace/raw共享异常时可以关闭对应优化开关并分别计算，但必须明确记录降级状态，不能把性能目标标记完成。

## 21. Risks / 风险

| 风险 | 处置 |
|---|---|
| 批量executor形成第二套业务逻辑 | 只编排既有权威组件；语义hash和完整golden逐日比较，发现差异即阻断 |
| batch buffer含未来行 | 业务内核只持有day view capability；未来毒化和直接访问测试阻断 |
| 历史日期后来回填/修订 | availability/revision/token/actual-read receipt共同约束；无token走immutable slice或full hash慢路径 |
| workspace跨日污染 | 静态只读、动态sandbox；完整内容key、写保护和cache poison测试 |
| raw共享把B增强混入A | raw/overlay明确分叉；raw-affecting registry未知字段fail closed |
| chunk失败越过名单顺序 | candidate预计算与list transition分层；首个失败日后不提交list/episode |
| 长事务影响DEV数据库 | chunk短只读事务读取后立即关闭；日级业务写独立事务 |
| 并发重新触发4.7GB进程资源争用 | 首版单worker；先降I/O/重复计算，资源验收后另议并发 |
| 性能优化未达到目标 | 如实标记性能未完成；不删安全逻辑、不虚报估算、不扩大为通用平台 |
| 当前v6证据被新代码混合 | v6先冻结，H0使用新code-release/run identity，禁止跨版本resume或覆盖 |
| 合入被误解为运行时切换 | merge、activation、restart和post-restart readback分别报告并由用户控制 |

## 22. DESIGN-COMPLIANCE-001

文档合入前和未来实现合入前均逐项证明：

1. **禁止简化交付**：不能用batch size=1、单日smoke、候选count或估算耗时冒充完整44日语义与性能验收。
2. **禁止静默错误**：cache miss/corruption、source drift、PIT越界、raw identity冲突和恢复冲突均typed failure；不得silent fallback到旧artifact、A组或逐日成功状态。
3. **禁止改变业务逻辑**：single/batch业务语义hash必须一致；H0不改变Selection、HMM、risk、名单、outcome、Program或binding语义。
4. **禁止私增门禁**：H0不增加角色、审批或模型激活门禁；golden、等价和资源检查是用户要求的实现验收，不阻断P0/P1模型主线。
