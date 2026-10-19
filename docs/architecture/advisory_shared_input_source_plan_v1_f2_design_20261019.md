# Advisory / LocalSIM 共享输入来源计划 F2 详细设计

日期：2026-10-19。分类 F2/T3；用户批准实现三种来源模式，源码限 Advisory ownership。继承 `advisory_shared_selection_signal_kernel_detailed_design_20261014.md`、`localsim_shared_frozen_file_inputs_20261017.md` 及 `selection_provider_clocks_v1`，不复制其算法。当前：源码与定向回归完成；真实文件/混合等价因原生 PIT 政策不一致阻断，整体 F2 尚未通过，不宣称可运行接续或可合入。

## Background / 背景

现有 `file_input_session`、`database_input_session` 已用同一原生特征 builder，但 CLI 一次运行只能选一个来源。文件截止后不能在冻结边界接续数据库；调用方各自实现会造成日期、缓存和重试不一致。现有 engine 已记录真实价格/特征/人口摘要；仅含数据库表名的身份不是数据内容证明。

## Scope / Non-goals / 精确范围

允许文件：本文；`backend/services/advisory_historical_range/input_source_plan.py`（新增公共纯合同与薄适配）；同目录 `inference_inputs.py`、`selection_signal_kernel.py`；`backend/inference_engine.py`；`scripts/strategy_package_live_inference.py`；对应直接测试 `backend/tests/advisory_historical_range/test_input_source_plan.py`、`test_strategy_package_live_inference.py`、`test_selection_signal_kernel.py`。只有发现必要性才改已有文件。临时计划、实测收据全部 X，不复制大资产。

禁止修改 LocalSIM、StrategyPackage、Selection、QE、HMM、data_service/数据发布、ownership、公共 CI。LocalSIM 分钟 reader/账户/账本/调度/恢复/UI 不在本交付；公共层只给来源解析及输入 session，消费者独立运行。无需数据库 schema、训练、数据补齐、生产回放或服务控制。

## Architecture / 架构

范围补充（实施前登记）：`backend/services/advisory_historical_range/batch_inference_provider.py` 的 opt-in `batch_dates(source_plan=...)` 和 `composition.py` 的执行源码闭包；直接测试 `backend/tests/advisory_historical_range/test_batch_inference_provider.py`。这使荐股适配器真实调用公共 CLI，不只交付孤立 helper；不扩展其它 owner 或业务 API。

任务创建 → 官方 `freeze_dataset_binding` 解析最新 active 发布（仅一次）→ 公共 `build_source_plan` 冻结日历、组件覆盖、policy、C/D/T → 原子保存调用方任务输入 → `resolve_day_source` → `create_input_session` → 唯一 `InferenceEngine` → 既有原生 prepare/computation + `SelectionSignalKernel`。两个消费者不交换输出、缓存或断点。

第一版只做完整日段切换。文件覆盖结束后的首个数据库日，整个模型历史窗口交现有 DB reader；不会把文件历史与 DB 尾部拼接。文件日真缺数不是切换触发条件，异常不能回退 DB。来源边界是已冻结元数据的保守交集，组件内部缺洞按实际读取失败，不裁剪日期。

## Contracts / 公共接口与身份

所需组件取自调用方冻结的 `native_policy.required_components`（daily/minute/factor/pit 必需；suspend/sector 按原生政策需要声明；未声明时保守包含全部）。不得为不使用行业过滤的调用凭空增加行业成员覆盖起点限制；也不得为了通过而省掉原政策使用的组件。共享 source factory 不根据股票/包 ID 猜能力。已提供的 `file_source` 在注册时与其冻结 binding 指向的小型权威元数据逐项核对，不接受修改覆盖字段后自行重算 plan hash。

- 模式 `FILE_ONLY` / `DATABASE_ONLY` / `FILE_THEN_DATABASE`。调用方提供 consumer_scope、run_id、有序唯一 C/D/T、原生政策与其 hash、完整请求交易日日历、每日允许的最早历史起点。同日多模型可提供所有原生窗口的共同最早边界；实际模型起点仍由原生 resolver 决定并写入实际输入收据，不强制各模型使用同一长度，不猜模型类别、因子窗口或包 ID。
- 文件计划冻结整个 binding、manifest/hash、日/分钟日历、因子、PIT、停牌、行业组件的声明覆盖。minute 覆盖影响整日边界，但不由此模块实现分钟执行。C 输入历史、D 停牌、C risk/行业/参考价、T 目标各保留；HMM 仍使用既有合法可见时钟，不生成系数。
- 公共建计划接口允许注入已由消费者官方注册取得的 binding；新任务默认调用官方 resolver，恢复只 `validate_source_plan`，不读 active profile。调用方保存完整计划而非仅截止日。计划必须绑定本 consumer/run；同输入另一消费者可独立创建计划，不能当成本次缓存命中。
- 文件日必须提供原 manifest 或请求内的显式原生 PIT identity；不能临时查询数据库活动指针补身份。共享代码只检查声明存在，完整身份仍由唯一原生 parser 校验，不自行构造权威收据。日历/模型输入 C、停牌 D、分钟覆盖 T 分别检查，不能要求 D 才消费的停牌组件必须覆盖前一 C。
- 政策含原生 policy hash、PIT universe/rule、已实现 reader 的单位/复权合同版本。文件 PIT 元数据与原生政策一致；不凭调用方字符串把不一致数据升级为匹配。数据库 reader 使用现有原生口径，执行时仍验证原生 PIT，不擅自修补。
- 计划 hash 是声明/边界身份，不冒充实际 DB 内容。每个执行日返回来源、计划 hash、C/D/T、文件 binding 或 DB 实际输入摘要及明示证据级别。真实价格/特征/人口摘要用于漂移比较；日历日期解析摘要只标日期解析，不声称全表内容 hash。
- 无可信不可变 DB revision 时，每 DB 日新 session（不跨日命中可变源），仍在日内复用读入分区；精确恢复对已完成 DB 日重新只读推理并比较实际输入摘要及完整评分，禁止直接信任旧断点。不覆盖旧产物。若不一致报 `SOURCE_INPUT_DRIFT`，由消费者新任务处理，不自动重算覆盖。
- 文件 cache 延续现有冻结 binding/stat lease，实际使用文件变化失败。bin stat lease 明示不是 producer 内容 hash。语义身份纳入计划和当日来源收据；已有未提供计划的单源调用/产物维持原合同，不强制迁移。
- 缺口异常带 reason_code、stage、日期、component、owner；缺数据交数据 owner，公共原生接口能力交对应 owner。不能把预检通过当成完整数据/经济成功。

## Implementation Plan / 实施方案

1. 登记本文、精确范围与验收索引；接口/日期/身份三视角审核。
2. 实现公共计划、验证、解析、session 工厂；CLI 历史只读入口接入，旧入口不变。Advisory 从同一公共入口消费，不另建私有来源算法。
3. 直接合同测试：三种模式、边界、原生窗口、冻结重试、漂移/失败、消费者隔离及原生时钟。
4. 原生 LSTM/TCN 只读 fresh-process 对照；无生产回放/训练。真实阻塞原样登记，不用 mock 代替。
5. 多轮审核修复、DESIGN-COMPLIANCE-001、F2 校验、精确 CI、独立 PR。真实关键验证若仍缺失，保留未完成状态，不假称合入就绪。

## Verification Plan / 验证方案

保持完整评分、顺序、TopK、UNKNOWN、原始人口、停牌资格及政策时钟，与同截止/同政策/同源原入口逐项对照。覆盖截止日前后连续日、首个 DB 日全历史窗口、active 更新不改旧计划、文件内部洞不回退、DB 输入漂移、失败再试、旧单源兼容。实测区分冷/热、端到端耗时、进程峰值内存、产物磁盘增长；资源不可用或基础数据缺口如实列 owner 需求。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-331 | 公开三模式计划/验证/逐日解析/session/缺口接口，消费者运行隔离 |
| F-332 | 最新合法发布一次冻结，覆盖交集、完整日切换与不可变重试 |
| F-333 | C/D/T、首 DB 日全历史及唯一原生 reader/评分/过滤/TopK |
| F-334 | 政策/单位/复权/PIT 匹配，缺口 fail closed、无异常回退 |
| F-335 | 来源与实际输入身份进入缓存/产物/恢复，旧单源兼容 |
| F-336 | 三模式及原生 LSTM/TCN 对照、真实资源测量与证据限制 |
| F-337 | LocalSIM 接口示例、owner 缺口、源码/运行/消费者分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-331 | input_source_plan.py；batch_inference_provider.py | backend/tests/advisory_historical_range/test_input_source_plan.py；test_batch_inference_provider.py | PASS | none |
| F-332 | input_source_plan.py | backend/tests/advisory_historical_range/test_input_source_plan.py | PASS | none |
| F-333 | inference_inputs.py / CLI / 原生 kernel | backend/tests/advisory_historical_range/test_input_source_plan.py；test_selection_signal_kernel.py | PASS | none |
| F-334 | input_source_plan.py / 原生 reader | backend/tests/advisory_historical_range/test_input_source_plan.py；真实readiness.json | PASS | none |
| F-335 | CLI / kernel / composition.py | backend/tests/advisory_historical_range/test_input_source_plan.py；test_strategy_package_live_inference.py | PASS | none |
| F-336 | 原生只读验证 | artifact: X:/AIstock_temp/ADVISORY/shared-source-plan-20261019/native-r3/progress.json；native-r2/progress.json | BLOCKED | 文件/混合真实等价和原生TopK仍缺；源政策不一致，不得改PASS或升级证据 |
| F-337 | 本文接入交付 | artifact: docs/architecture/advisory_shared_input_source_plan_v1_f2_design_20261019.md | PASS | none |

## Rollout / Rollback / Production Gates

新接口 opt-in；旧任务不升级。失败保留旧产物，用新任务身份处理变更，不覆盖/重置账户。PR 合入不等于运行加载；后端用户重启后只读核对。LocalSIM owner 独立接入及接续验收，不运行本窗口生产回放。数据库/数据发布/训练/服务控制均零操作。

## Risks / 失败模式

文件覆盖声明不保证每股每字段齐全；历史 PIT 与新包政策可能不匹配，应报告而不是降级。DB 缺乏统一 revision 时重复验证成本较高；用实际输入摘要保证可检测漂移，不承诺跨连接事务快照或历史 vintage。混合模式不保证文件与当前修订 DB 数值逐位相同，两源对照报告合法修订差异，不调容差掩盖。native 无公开适配能力时交对应 owner，不越界修复。

## Review / DESIGN-COMPLIANCE-001

第一轮设计自审：覆盖边界不使用异常判定；全历史 DB 日不拼接；C/D 时钟不统一改写；现有两 session 与算法唯一。

第二轮源码自审修复：外部 metadata 注入须与原绑定权威数据一致；raw session 同样限制合法 C；不同模型保留自己的原生历史长度；新增 batch adapter 和源码闭包登记；本次源计划身份、实际输入和原 native context 共同参与漂移校验。58项最小稳定矩阵通过，追加身份变更2项定向回归通过。

第三轮时钟/兼容自审修复：停牌覆盖按 D、分钟按 T；纯文件模式不借 live DB pointer 补 PIT；保留旧单源请求；exact retry 检测 DB 输入漂移而不覆盖已完成日。所有修改仍限精确 Advisory 文件，不宣称独立外审。

DESIGN-COMPLIANCE-001 四项：①未简化交付，F-336明确BLOCKED，整体不得报完成；②已核对显式失败/无回退/原日不覆盖/UNKNOWN保留，定向测试通过；③唯一原生评分、过滤与时钟不变，新增 opt-in 来源合同及实际范围已同步本文；④控制只实现用户要求的政策/内容/日期一致性，无包白名单、再审批、训练准入或私增策略门禁。source merge、runtime loading、LocalSIM 验收均未完成，不能互相替代。

## 公共接入示例 / LocalSIM handoff

以下为代码接口示例，不启动账户、回放或任何正式任务。`native_policy` 的 universe/rule/policy hash 必须来自调用方冻结政策；不能为了通过文件校验改写为文件里的值。

```python
from contextlib import ExitStack
from backend.services.advisory_historical_range.input_source_plan import (
    build_source_plan, load_source_plan, resolve_day_source, create_input_session,
)

# 新任务一次：官方 resolver 读取配置的 active 发布；由本消费者自己的
# 原子任务 writer 保存完整 plan，后续按保存的 plan_hash 恢复。
plan = build_source_plan(
    mode="FILE_THEN_DATABASE", consumer_scope=consumer_scope, run_id=run_id,
    days=explicit_C_D_T_and_history_bounds, trading_calendar=native_calendar,
    native_policy=native_policy,
)
# 恢复：仅加载原计划，不传 active profile，不重新决定边界。
plan = load_source_plan(saved_path, consumer_scope=consumer_scope, run_id=run_id)
route = resolve_day_source(plan, clocks)
with ExitStack() as stack:
    inputs = create_input_session(
        plan, clocks, consumer_scope=consumer_scope, run_id=run_id,
        max_bytes=input_budget, root_override=verified_same_release_node_locator,
    )
    stack.callback(inputs.close)
    engine = InferenceEngine(input_session=inputs, ...)  # 原模型/因子/processor
    # 原 run_inference(... persist_signals=False, universe_ensure=False ...)；
    # receipt.input_context.source_plan_receipt 带真实输入摘要。
```

CLI：原 `--historical-plan <本次子块>` 增加 `--source-plan-file <全任务冻结计划>`、`--run-id <本消费者run>`、`--input-revision <plan_hash>`；仍需 `--historical-read-only`、X 盘 `--temp-root`。`--dataset-root` 只改同发布的节点 locator，不改变内容身份；不得同时传旧 `--dataset-binding`。节点 COMPLETE 仅表示请求子块完成，不代表完整账户/任务完成。

荐股适配：`AdvisoryBatchInferenceProvider.batch_dates(..., source_revision=plan["plan_hash"], source_plan=plan)`。原生请求 C 必须与计划一致；适配器不把 C/D/T 统一改写。日期绑定的冻结 workspace 仍逐日消费。LocalSIM 不调用此荐股私有 adapter，直接消费上面的公共函数及同一个 CLI/engine/kernel；source plan hash 传入 `SelectionSignalRequestV1.source_identity`，保留原生 prepared artifact 的 `artifact_input_context.source_plan_receipt`。

LocalSIM owner 需接上自己的任务持久化与来源路由：文件日用既有文件 provider；DB 日全历史走原 DB reader；分钟行情/停牌估值/账户/账本/断点仍归其 owner。不能仅在模型推理切换来源而把其余 provider 偷偷留在另一来源；须让自身所需组件覆盖进入同一计划，并自行验收 D 停牌、C risk/行业/参考价。旧任务/旧产物不自动改计划；DB checkpoint 的实际输入验证不可省略。

## 当前验证与 owner 缺口（2026-10-19）

直接相关 58 项测试 PASS，随后身份摘要修订的2项定向回归 PASS（属于58项内的子集，不合计为60项）；Ruff PASS。提交后对全部11个精确文件重新扫描：ownership 11/11 mapped、0 ambiguous/unmapped；L0 0 blocking、1项P2提示。该提示位于 batch provider 的命令参数字符串 `join`，不是股票×日期×特征的关联运算；实际调度复用同一 workspace/source-plan 执行，日期绑定 workspace 仍按原生身份逐日运行，不承诺跨日消除这部分工作。此前 changed-only 扫描0 finding不替代本次完整精确范围结果。是同窗口多轮自审与定向回归，不冒称独立外审。涵盖三模式、C<D、整日切换、首DB日全窗口、active更新不重解、内部缺洞不回退、旧单源兼容、数据库checkpoint漂移与原日文件不覆盖；完整生产两端结果尚未证明。

活动文件发布只读解析耗时47.70秒（包含现有官方 H5 content pin 的一次读取，无文件复制），binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`；该 hash 仅说明本次冻结内容，不是模型收益或运行激活。

| 来源 | universe_key | rule_version | 覆盖 |
|---|---|---|---|
| 活动文件发布 | aistock_equity_pit_canonical_v2 | shsz_a_252td_st_delist_asof_v2 | 2018-08-01..2026-08-31；分钟自2024-01-02，行业成员自2024-07-01 |
| 原生运行指针只读查询 | shsz_st_pit_active_v1 | st_pub_next_trade_restore_active_l_v1 | 2018-08-01..2026-10-19 |

真实 LSTM/TCN 原 manifest 未声明另一显式推理 PIT 身份，当前走原生指针。两种含文件模式均明确 `SOURCE_PIT_POLICY_MISMATCH`；没有篡改原政策、补成员或推进到假评分。旧代表 workspace 的首轮 node probe 未传既有公共 safe_artifact_roots 参数，因此在资产路径检查退出，非模型/数据错误；已用原公共 provider 的参数登记该只读 workspace 和原资产根重新验证，无生产白名单或策略包特例。

只读数据库原始推理对照（C=2025-12-31，D/T=2026-01-05；不调用生产回放，不训练）：

| 模型/收据 | 基线/新入口端到端秒 | 基线/新入口峰值RSS KiB | 完整评分/人口/UNKNOWN | 证据边界 |
|---|---|---|---|---|
| LSTM `native-r3` | 451.09 / 520.91 | 7759248 / 7712068 | 4905 / 4911 / 6 | 源码提交 `9ce67d9a80ea4c3657f22f00e1beb1f0024fb00b`，两臂COMPLETE、10个源码内容身份全程不变；评分及顺序、成员、UNKNOWN、实际输入摘要逐项相同 |
| TCN `native-r2` | 502.30 / 464.99 | 7614588 / 7711912 | 4905 / 4911 / 6 | 两臂COMPLETE、同样五项相同；发生在最终文件身份护栏及context摘要修订前，只作早期源码诊断，不伪绑最终HEAD |

LSTM 实际模型窗口从2024-11-05开始，由原生 resolver 决定；没有截短为单日。`native-r2` LSTM 因运行期间源码审阅发生修改，被原生源码闭包校验拒绝；该次失败不计入通过，不覆盖。`native-r3` 单独在固定源码下重做。

磁盘：LSTM 两臂结束后各保留454155/457082字节；新入口运行中 `du -sk` 抽测1629540 KiB（约1.55 GiB，统计原生临时特征文件，不跟随大型模型链接）。这只是观测到的占用，**不是从启动到退出的完整峰值**，后续完整混合验收需补全程测量。没有复制大型模型或数据集。单次共享负载下耗时一升一降，不证明性能提升，也不能据此定性所有日的性能回归；未作冷/热重复分组性能验收。

独立草稿 PR #6356；源码提交 `9ce67d9a8` 的 CI run `39232982569` verdict通过。F2验证仍明确拒绝 F-336，CI绿不等于真实混合来源/原生TopK/停牌资格和LocalSIM接续验收通过。后续文档收据提交不改上述推理源码，也不把旧测量改绑新HEAD。

精确交接：请 Selection/PIT authority owner 确认这两个已批准包的正式推理应消费哪一原生 PIT 政策，并提供现有公开身份合同；数据 owner 只在该政策确定后提供同政策合法文件发布或确认现有匹配资产。不要求本窗口切换指针、改股票池、重训或改包；不得以两套政策名称不同就推定数据缺失。问题是**跨来源政策无法证明一致**。源码验证、真实全评分/TopK/UNKNOWN/停牌等价及 LocalSIM 接续分别保留状态，F-336 暂未满足，不可宣布可合入/运行验收完成。
