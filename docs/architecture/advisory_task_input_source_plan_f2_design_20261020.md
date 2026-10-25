# Advisory 实际任务接入共享输入来源计划

日期：2026-10-20；F2/T3。用户已批准按优先级接续实现。继承
[公共来源计划](advisory_shared_input_source_plan_v1_f2_design_20261019.md)
和 [共享选股内核](advisory_shared_selection_signal_kernel_detailed_design_20261014.md)。

## Background / 当前事实

PR #6356 已交付公共三模式、CLI 和 batch provider 的 opt-in 参数，R15
真实 LSTM/TCN 跨界、原生内核及恢复通过；本轮实现前的缺口是实际历史任务
创建/produce_dates尚未传入来源计划。本轮已接通并完成下述只读技术验证，
不是数据/PIT 阻断，不重建数据或重复完整 R15 实验。

## Scope / 精确写范围

允许 `backend/services/advisory_historical_range/` 下：`api_models.py`、
`models.py`、`service.py`、`planning_service.py`、`repository.py`、
`requirement_planner.py`、`candidate_producer.py`、`composition.py`、
`batch_inference_provider.py`、新增 `task_input_sources.py`；对应直接测试
`backend/tests/advisory_historical_range/test_task_input_sources.py`（新增）、
`test_r2b_candidate_producer.py`、`test_repository.py`、`test_r5_command_service.py`、
`test_batch_inference_provider.py`，以及本文、荐股蓝图与同目录研究假设说明。
若必要文件超出此列表，先补登记理由。仅修改实际必要文件。

范围补充（实现前）：`source_roles.py` 登记计划身份为候选来源；新模式不再登记或扫描
选股 DB calendar/PIT 全量内容，实际输入由原生收据绑定；私有 decision marks 保留自己
两类原行情/状态来源，不借选股来源作证明。`catalog_postgres.py`
仅扩展现有 Program selector 匹配函数，使同包多 Program 不串用计划。不改查询或调度。

恢复范围补充（实现前）：`executor.py` 在复用本任务未提交 candidate artifact 时调用
source-aware producer 重新验证实际输入/原生过滤，再比对完整旧候选内容；旧产物不覆盖。
相应直接回归登记在 `test_task_input_sources.py`。已提交的经济前缀不重做。

## Non-goals / 非目标

禁止修改 LocalSIM、Selection、StrategyPackage、QE、HMM、数据发布或公共 CI。
复用已有公开 FrozenDataset、文件行情 adapters 和原生风险/过滤实现，不复制算法。
无数据库 schema/生产写入、训练、生产回放、服务控制或大型资产复制。

## Architecture / 方案

1. 创建 API 增加可选 `selection_input_source_mode`：FILE_ONLY、DATABASE_ONLY、
   FILE_THEN_DATABASE。缺省/null 保持旧单源请求；显式选择即接通新模式，不需新审批。
   该字段属于用户语义/idempotency，旧 JSON/hash 在字段缺省时逐字义兼容。
2. 任务规划在冻结包、原生历史窗口和日历后，只解析一次最新文件发布，并为各 Program
   构建公共 source plan。计划放入 frozen Program 的可选 `selection_input_source_plan`，
   由现有 planning/request/program JSON 和内容寻址 writer 原子持久化，不加表/DDL。
   source sub-run 在 seal 前由本请求/Program/日历/代码确定；range_run 仍由 sealed hash
   派生，故不存在 plan hash 与 range_run 相互依赖。两者关系在产物中显式记录。
3. 原历史信号入口的 C=D=T 映射保持不变；它不等于荐股业务的下一交易日执行计划。
   后续显式 C<D 仍走公共内核原合同，不能借本任务重写旧交易日。
4. 新计划的 selection market 输入不再进行旧的数据库全历史候选预扫描；catalog
   保存真实的 **计划声明身份**，不得称其为行情内容 hash。实际内容由唯一原生 CLI
   的四类 source_read_receipts、完整窗口和 source_plan_receipt 验证并进入原始及候选身份。
   HMM、代码和包资产仍沿原冻结合同。旧任务继续旧 catalog 验证，不降级旧政策。
5. produce_dates 传入同一完整冻结计划；单日直接调用同样建立来源 scope，不得只有批量
   才支持新模式。文件日原生 tradability/risk 使用该文件发布的现有 adapters；DB 日使用
   既有只读 providers。HMM 仍消费已冻结合法资产，不生成系数。
6. 新计划信号进入真实 SelectionSignalKernel.run，不另写排序/TopK。原始评分、人口、
   UNKNOWN、实际 provider 日期/receipt、source plan 和实际输入摘要绑定候选原生身份。
   参考价仍不在 raw 历史推理中伪造；私有候选 projector 和名单规则不变。
7. 创建 exact retry 先读现有 request key，验证用户语义并返回已有 durable 操作；不重读
   active profile。执行/恢复只用存下的计划。自身原生输出按源计划/工作区/日期块保存，
   同一物理 workspace 的重试走原 CLI resume；原生 workspace 若被其 owner 释放/重建，
   新 attempt 独立计算并与本侧旧候选完整比对，不伪装相同 path-bound CLI checkpoint。
   不复制/保活大型 workspace 资产。DB 无不可变 revision 时仍实际重读摘要，不信表名 hash。
   旧输出不覆盖；不同 plan/provider 身份不能命中旧 C-filter 或旧源断点。
8. 原生StrategyPackage v2规范化上下文的字段/hash保持原合同。单腿原始来源上下文由本侧
   batch provider保存在既有provider metadata中，校验其日期/窗口/PIT/日历/人口与规范化
   上下文一致，再校验完整原生source receipt；多腿沿原生component_artifacts逐腿验证。
   不修改StrategyPackage的规范化器，也不向旧v2上下文倒填字段或重签其hash。

## Contracts / 范围与失败

`selection_input_source_mode` 只选择 **共享选股信号输入**，不是整个 Advisory 任务的数据源。
任务登记、包解析和 HMM 元数据允许只读控制面数据库；Advisory 私有的 decision marks、
名单复评和收益分析继续各自已有显式来源合同。不能把 selection FILE_ONLY 宣称为这些
私有行情热路径已全部零 SQL，也不向 LocalSIM 共享其业务结果/缓存。

PIT authority/rule/parameters 与当前 canonical 政策一致，保留物理 snapshot/release 身份；
使用现有 public helper，不设置新的策略包准入。文件范围内真缺数、政策不一致、非法窗口、
来源/hash 漂移或 DB 真缺数明确失败，无异常到 DB 的自动 fallback，不删日期/股票。

## Implementation Plan / 实施方案

先接可选创建参数与frozen Program；复用现有CAS/规划持久化和读回。随后接原生
batch provider、实际kernel和逐日provider，再接未提交候选恢复/漂移比较；最后
补直接回归、真实短窗、LocalSIM接口交接和本轮蓝图状态。不拆新平台或复制算法。

## Verification Plan / 验证方案

先 RED→GREEN 覆盖 API→domain 透传、创建冻结和 exact retry、旧序列化/身份、三模式
逐日路由和首 DB 日完整历史、单日/批量一致、原生真实 kernel、provider 时钟、失败恢复。
再补最短真实 native consumer 接入验证：复用 R15 已证明的底层输出/基线，不重复训练
或 20 日生产回放；测试替身不冒称新真实推理。记录真实耗时/资源与未覆盖边界。

多轮本窗口审核分别检查接口/身份、PIT/时钟、恢复/兼容和最终 diff；不冒称独立外审。
最终精确测试、Ruff、ownership、DESIGN-COMPLIANCE-001、F2 和必需 CI 通过后按已有
授权提交/合入/本任务清理。后端由用户重启；源码合入、fresh-process 技术验证、运行加载、
LocalSIM owner 接续验收各自报告。LocalSIM 私有代码仅提供接口说明。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-341 | 真实创建入口支持三模式，缺省旧 API/JSON/hash 兼容 |
| F-342 | 一次冻结并原子存储来源计划，精确创建重试不重解 active |
| F-343 | 单日/批量/恢复沿唯一 CLI、原生模型及真实 kernel，完整输入证据 |
| F-344 | 文件/DB providers 同源，C/D/T、PIT、UNKNOWN、排序/TopK 不变 |
| F-345 | 计划声明与实际内容分离，漂移/失败/旧产物隔离明确 |
| F-346 | 直接回归、最短 native 接入和 LocalSIM handoff，运行分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-341 | api_models.py / models.py / service.py | backend/tests/advisory_historical_range/test_task_input_sources.py | PASS | none |
| F-342 | planning_service.py / repository.py / task_input_sources.py | backend/tests/advisory_historical_range/test_task_input_sources.py | PASS | none |
| F-343 | candidate_producer.py / batch_inference_provider.py | backend/tests/advisory_historical_range/test_task_input_sources.py；test_batch_inference_provider.py | PASS | none |
| F-344 | task_input_sources.py / shared kernel | backend/tests/advisory_historical_range/test_task_input_sources.py；test_selection_signal_kernel.py | PASS | none |
| F-345 | frozen Program / catalog / native receipt | backend/tests/advisory_historical_range/test_task_input_sources.py；test_r3_executor.py | PASS | none |
| F-346 | this document / exact validation / LocalSIM handoff | artifact: X:/AIstock_temp/ADVISORY/source-plan-task-integration-20261020/final-direct-r5.xml；同目录native-r9-lstm/consumer-recovery-r2.json及native-r9-tcn/consumer-recovery-r2.json | PASS | none |

### 真实技术验证与结论边界

124项定向回归通过（18.63s），覆盖上述接口、真实repository方法的只读创建重试、
单日/批量kernel、三模式、漂移和未提交恢复；production planning存储未写入。
真实短窗使用原生LSTM/TCN及完整冻结processor/Alpha158/动态因子：

| 项目 | LSTM | TCN |
|---|---|---|
| FILE末日2026-08-31，C=D=T | 4919评分/4919人口/0 UNKNOWN | 4919/4919/0 |
| 首DB日2026-09-01，C=D=T | 4911评分/4911人口/0 UNKNOWN | 4911/4911/0 |
| 同输入原生基线比较 | 完整原始评分/排序、Top50一致 | 完整原始评分/排序、Top50一致 |
| fresh native resume后实际重读及候选恢复 | PASS，396.52s | PASS，388.67s |
| 上述恢复native进程峰值RSS | 8,909,712KiB | 8,880,384KiB |

首DB日整个279会话历史从DB读取，不接续文件半窗口。全量原始score按原精度比较；
私有CandidateFact沿既有12位小数持久化合同，未改变精度规则。原生两日推理首次分别
约588.21/580.47s；其输出已真实生成候选。最终比较明确复用**本run既存COMPLETE原始输出**，
不是另一个新预测；随后撤掉诊断复用入口，真实CLI resume重新读取/推理实际输入，
逐日重验旧候选完整内容并得到相同CAS引用。未覆盖、重签或替换旧产物。

短窗开启原生停牌过滤，HMM/risk关闭、行业黑名单为空；不宣称本次新测了所有ON组合。
真实规划/catalog与原生推理/consumer/CAS运行，但planning repository用内存替身，
生产任务创建持久化未执行；该边界由真实repository方法定向测试及已有事务合同覆盖。
R15为另一合法时钟输入，不将其数值移植到本轮C=D=T，也不宣称普遍单日性能SLA。
本轮未训练、写库、控制服务或执行生产回放；仅28,429/28,961字节小型冻结源码
放X盘，大型权重/processor/数据原位只读。运行加载需用户重启，LocalSIM由其owner
依下述公共接口接入并独立验收；技术PASS不是收益确认。

## Risks / Rollout / Rollback

显式新模式仅影响新建任务，不热迁移旧任务，不覆盖已完成经济前缀。回滚源码时保留新产物，
旧代码不消费不认识的新计划。FILE_ONLY 不规避原政策真实需要但文件未覆盖的行业维度。
数据库 mutable revision 限制、私有 decision-mark SQL 和用户重启等待如实报告。

## Production Gates / 生产边界

本轮不执行DDL/DML、生产数据补齐/激活、QE训练、生产回放或服务控制。
既有授权只交付审核/测试/必需CI通过的本任务源码；backend-main加载需用户重启。
fresh-process只读技术验证不是生产任务持久化或LocalSIM账户接续验收；不因等待
重启/UI或其它窗口而重跑已完成模型或阻断后续离线研究。没有新增策略包准入条件。

## LocalSIM owner 接入说明

公共接口仍是 `backend/services/advisory_historical_range/input_source_plan.py`，
不要求LocalSIM依赖本次私有 `task_input_sources.py`、Advisory Program、收益或名单容器。
不新增LocalSIM适配器副本，不共享计算结果、业务缓存、账户、账本或断点。

| 操作 | 公共入口 | 调用方责任 |
|---|---|---|
| 新任务冻结 | `build_source_plan` | mode、consumer_scope=`LOCALSIM`、自身run_id、C/D/T、完整原生历史起点、日历及原生policy；用自身原子任务存储保存完整返回值 |
| 恢复校验 | `validate_source_plan` / `load_source_plan` | 只读已保存计划，绑定自身scope/run；不调用build重新解析active，不覆写旧计划 |
| 逐日路由 | `resolve_day_source` | 传当天合法C/D/T；来源由冻结覆盖决定，不用异常触发fallback |
| 原生输入 | `create_input_session` | 同计划/scope/run/日期，预算由调用方给出；接原生InferenceEngine，使用后close；FILE/DB reader与CLI共用 |
| 实际证据 | `input_receipt` / `gap_receipt` | 保留原生四角色完整摘要/窗口与明确失败；不得用计划或表名hash冒充数据内容 |

推荐沿现有唯一CLI，不复制其session/processor/序列/评分装配：

```python
from backend.services.advisory_historical_range.input_source_plan import (
    build_source_plan, validate_source_plan, resolve_day_source,
)

# creation_args来自LocalSIM自己的合法冻结政策、日历和任务，不引用另一个场景的run。
plan = build_source_plan(consumer_scope="LOCALSIM", run_id=own_run_id, **creation_args)
# 由LocalSIM owner将plan原子写入自己的任务元数据；这里不跨owner实现存储。

# retry载入原plan，不重新build、不解析当日active profile。
validate_source_plan(saved_plan, consumer_scope="LOCALSIM", run_id=own_run_id)
route = resolve_day_source(saved_plan, own_day_clocks)
```

既有CLI请求增加 `--source-plan-file <本run保存的计划>`，同时传
`--consumer-scope LOCALSIM --run-id <自身run> --input-revision <plan_hash>`，
以及自身 `--historical-plan` / X盘 `--temp-root` / `--output-path`；多模型同日保留
各自request_id和原生模型workspace，不能覆盖另一包/消费者的结果。已有文件发布在WSL
使用其已验证节点路径；不硬编码本文验证日期、包ID、临时root或源码工作树。

第一DB日的**整个模型历史窗**走既有DB reader；不复用文件截断窗，不改复权/单位。
停牌D、risk/行业/参考价C继续公共provider时钟合同。LocalSIM分钟行情、日计划、
旧账户恢复及UI仍由该owner负责；须分别做其真实链路接续验收，Advisory技术PASS不能替代。
旧单源接口和既存产物保持可读；新模式仅显式新任务选择，旧任务不热迁移。

## Review / DESIGN-COMPLIANCE-001

设计第一轮：消除 source run 与 sealed run 的循环身份；明确单日入口与私有行情范围，
不把计划 hash 冒称内容身份；创建重试核对DB行对应的真实artifact hash及根身份。

实现第二轮：RED发现过滤入口未校验额外suspend metadata pin，已复用公开content_pin
补齐；嵌套单日scope复用外层prefetch，部分原生checkpoint不覆盖；未提交候选在私有
marks/经济提交前重读并比对，已提交前缀不重做。补program selector防同包不同Program串源。

实现第三轮：真实native推理完成而消费失败，定位到v2规范化器有意不保留新增上下文。
在Advisory provider metadata保留原文并验证与规范化字段的一致性；RED→GREEN通过，
未修改StrategyPackage规范化器/hash，也不跳过原生收据验证。验证目录的跨系统链接、
路径大小写及遗漏小型冻结loader是测试工作区准备问题，已独立解决，不放宽路径政策。

实现第四轮：真实文件窗口使用frozen_file_calendar，DB窗口使用trading_calendar；
按原生来源做精确标识映射，四个RED→GREEN用例及两包四日真实收据通过。
未放宽历史起点/窗长、日期、来源或内容校验，也未改其它owner的日历实现。

最终本窗口自审：124项精确回归、Ruff、17文件ownership（0未映射/歧义）通过；
两包跨界及真实fresh retry通过后F-346才记PASS。不冒称独立外审，必需CI仍按最终PR HEAD执行。

逐项检查：①按登记范围完整实现，源码、真实技术验证与运行加载分态；
②真缺数、身份/日期/内容漂移明确报错，不回退、补值、删股或空成功；
③唯一原生评分/融合/过滤/递补/TopK及C/D/T、旧请求/hash/产物/经济前缀保留；
④没有新策略包准入/训练审批平台，没有其它owner源码、生产写入或服务控制。
