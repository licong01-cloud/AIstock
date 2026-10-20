# Advisory / LocalSIM 共享输入来源计划 F2 详细设计

日期：2026-10-19；接续更新：2026-10-20。分类 F2/T3；用户批准实现三种来源模式，源码限 Advisory ownership。继承 `advisory_shared_selection_signal_kernel_detailed_design_20261014.md`、`localsim_shared_frozen_file_inputs_20261017.md` 及 `selection_provider_clocks_v1`，不复制其算法。

当前进度（R15，覆盖下文较早时点）：数据 owner 已完成全局 canonical PIT 激活，本窗口 fresh-process 独立读回 `ACTIVE_CANONICAL`、generation=1、统一 key/rule，并确认 5,346只股票、5,665个区间、无效/重叠/规则混用均为0。原外部指针阻断已解除，现有 v18 文件不需重建。同步最新主线后的66项定向回归、静态及ownership检查通过；原生 LSTM/TCN 数据库基线、文件→数据库跨界推理、真实选股内核及精确恢复全部通过，F-336已闭合。源码交付进入PR最终检查；源码合入、运行加载和LocalSIM私有接入以各自交付/验收结果为准，不因共享接口通过而自动宣称两端生产激活。本窗口未重启或运行生产回放。

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
- 文件日从已冻结发布的原生 PIT 成员区间按 C 解析成员，构造现有 frozen-candidate 消费声明，并交唯一原生 parser 校验；这不是伪造 activation/receipt。声明绑定发布、成员文件内容 hash 和 C，不能使用今天的全市场成员替代历史成员。已有显式 request/manifest 身份必须与该发布/C/成员完全一致，不能静默覆盖。全过程不查询数据库活动指针。日历/模型输入 C、停牌 D、分钟覆盖 T 分别检查。
- 政策身份与物理存储身份分开：canonical authority、rule version、parameters digest 必须一致；合法 rolling key 与发布 snapshot key 不要求字符串相等。复用主线公共 canonical 校验，不增加第二种 PIT 算法。文件物理 key、发布 hash、实际成员摘要仍分别保留。数据库日注册时冻结官方活动 canonical 运行身份；未完成全局激活不能伪造 generation/envelope，也不能继续借 legacy 指针运行新任务。
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
| F-334 | input_source_plan.py / 原生 reader | backend/tests/advisory_historical_range/test_input_source_plan.py；artifact: X:/AIstock_temp/ADVISORY/shared-source-plan-20261019/native-r15/source.json | PASS | none |
| F-335 | CLI / kernel / composition.py | backend/tests/advisory_historical_range/test_input_source_plan.py；test_strategy_package_live_inference.py | PASS | none |
| F-336 | 原生只读验证 | artifact: X:/AIstock_temp/ADVISORY/shared-source-plan-20261019/native-r15/progress.json；native-r15/kernel/receipt.json；native-r15/retry.json | PASS | none |
| F-337 | 本文接入交付 | artifact: docs/architecture/advisory_shared_input_source_plan_v1_f2_design_20261019.md | PASS | none |

## Rollout / Rollback / Production Gates

新接口 opt-in；旧任务不升级。失败保留旧产物，用新任务身份处理变更，不覆盖/重置账户。PR 合入不等于运行加载；后端用户重启后只读核对。LocalSIM owner 独立接入及接续验收，不运行本窗口生产回放。数据库/数据发布/训练/服务控制均零操作。

## Risks / 失败模式

文件覆盖声明不保证每股每字段齐全；历史 PIT 与新包政策可能不匹配，应报告而不是降级。DB 缺乏统一 revision 时重复验证成本较高；用实际输入摘要保证可检测漂移，不承诺跨连接事务快照或历史 vintage。混合模式不保证文件与当前修订 DB 数值逐位相同，两源对照报告合法修订差异，不调容差掩盖。native 无公开适配能力时交对应 owner，不越界修复。

## Review / DESIGN-COMPLIANCE-001

第一轮设计自审：覆盖边界不使用异常判定；全历史 DB 日不拼接；C/D 时钟不统一改写；现有两 session 与算法唯一。

第二轮源码自审修复：外部 metadata 注入须与原绑定权威数据一致；raw session 同样限制合法 C；不同模型保留自己的原生历史长度；新增 batch adapter 和源码闭包登记；本次源计划身份、实际输入和原 native context 共同参与漂移校验。58项最小稳定矩阵通过，追加身份变更2项定向回归通过。

第三轮时钟/兼容自审修复：停牌覆盖按 D、分钟按 T；纯文件模式不借 live DB pointer 补 PIT；保留旧单源请求；exact retry 检测 DB 输入漂移而不覆盖已完成日。所有修改仍限精确 Advisory 文件，不宣称独立外审。

DESIGN-COMPLIANCE-001 四项（R15最终复核）：①按登记范围完整交付公共接口、CLI、荐股opt-in适配、直接回归和真实LSTM/TCN跨界/内核/恢复证据，F-331–337均PASS；不把未包含的业务API默认迁移或LocalSIM私有适配宣称已实现。②显式失败/无异常回退/旧产物不覆盖/UNKNOWN保留，漂移及失败恢复回归通过；原生exact retry确认原评分及commit字节不变。③唯一原生评分、过滤、递补、TopK及C/D/T不变；新增来源合同、同日session生命周期修复和文档示例修订已同步设计。④仅实现用户要求的政策/内容/日期一致性，无包白名单、再审批、训练准入或私增策略门禁。source merge、runtime loading、LocalSIM接续分别报告，不能互相替代；多轮审核是同窗口自审，不冒称独立外审。

## 公共接入示例 / LocalSIM handoff

以下为代码接口示例，不启动账户、回放或任何正式任务。`native_policy` 的 policy hash 来自调用方冻结选股政策；新任务的 PIT authority/rule 使用全局 canonical 政策，物理 release/snapshot 身份由冻结发布解析。不得用 legacy 股票池作 canonical 对照，也不能为通过测试修改已登记旧任务的政策。

```python
from contextlib import ExitStack
from backend.inference_engine import InferenceEngine
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
    engine = InferenceEngine(input_session=inputs, temp_root=consumer_temp_root)
    # 原 run_inference(... persist_signals=False, universe_ensure=False ...)；
    # 沿用原模型/因子/processor、只读参数和截止日，不在此重建请求。
    # 成功后 engine.last_inference_receipt["input_context"]["source_plan_receipt"]
    # 带真实输入摘要；旧的返回值合同不变。
```

CLI：原 `--historical-plan <本次子块>` 增加 `--source-plan-file <全任务冻结计划>`、`--run-id <本消费者run>`、`--input-revision <plan_hash>`；仍需 `--historical-read-only`、X 盘 `--temp-root`。`--dataset-root` 只改同发布的节点 locator，不改变内容身份；不得同时传旧 `--dataset-binding`。节点 COMPLETE 仅表示请求子块完成，不代表完整账户/任务完成。

荐股适配：`AdvisoryBatchInferenceProvider.batch_dates(..., source_revision=plan["plan_hash"], source_plan=plan)`。原生请求 C 必须与计划一致；适配器不把 C/D/T 统一改写。日期绑定的冻结 workspace 仍逐日消费。LocalSIM 不调用此荐股私有 adapter，直接消费上面的公共函数及同一个 CLI/engine/kernel；source plan hash 传入 `SelectionSignalRequestV1.source_identity`，保留原生 prepared artifact 的 `artifact_input_context.source_plan_receipt`。

LocalSIM owner 需接上自己的任务持久化与来源路由：文件日用既有文件 provider；DB 日全历史走原 DB reader；分钟行情/停牌估值/账户/账本/断点仍归其 owner。不能仅在模型推理切换来源而把其余 provider 偷偷留在另一来源；须让自身所需组件覆盖进入同一计划，并自行验收 D 停牌、C risk/行业/参考价。旧任务/旧产物不自动改计划；DB checkpoint 的实际输入验证不可省略。

## 首轮验证记录（2026-10-19，R1–R10，非最终源码验收）

直接相关 58 项测试 PASS，随后身份摘要修订的2项定向回归 PASS（属于58项内的子集，不合计为60项）；Ruff PASS。提交后对全部11个精确文件重新扫描：ownership 11/11 mapped、0 ambiguous/unmapped；L0 0 blocking、1项P2提示。该提示位于 batch provider 的命令参数字符串 `join`，不是股票×日期×特征的关联运算；实际调度复用同一 workspace/source-plan 执行，日期绑定 workspace 仍按原生身份逐日运行，不承诺跨日消除这部分工作。此前 changed-only 扫描0 finding不替代本次完整精确范围结果。是同窗口多轮自审与定向回归，不冒称独立外审。涵盖三模式、C<D、整日切换、首DB日全窗口、active更新不重解、内部缺洞不回退、旧单源兼容、数据库checkpoint漂移与原日文件不覆盖；完整生产两端结果尚未证明。

活动文件发布只读解析耗时47.70秒（包含现有官方 H5 content pin 的一次读取，无文件复制），binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`；该 hash 仅说明本次冻结内容，不是模型收益或运行激活。

| 来源 | universe_key | rule_version | 覆盖 |
|---|---|---|---|
| 活动文件发布 | aistock_equity_pit_canonical_v2 | shsz_a_252td_st_delist_asof_v2 | 2018-08-01..2026-08-31；分钟自2024-01-02，行业成员自2024-07-01 |
| 首轮原生运行指针（历史，非当前） | shsz_st_pit_active_v1 | st_pub_next_trade_restore_active_l_v1 | 2018-08-01..2026-10-19 |

首轮真实 LSTM/TCN 原 manifest 未声明另一显式推理 PIT 身份，当时走原生指针。两种含文件模式均明确 `SOURCE_PIT_POLICY_MISMATCH`；没有篡改原政策、补成员或推进到假评分。旧代表 workspace 的首轮 node probe 未传既有公共 safe_artifact_roots 参数，因此在资产路径检查退出，非模型/数据错误；已用原公共 provider 的参数登记该只读 workspace 和原资产根重新验证，无生产白名单或策略包特例。

只读数据库原始推理对照（C=2025-12-31，D/T=2026-01-05；不调用生产回放，不训练）：

| 模型/收据 | 基线/新入口端到端秒 | 基线/新入口峰值RSS KiB | 完整评分/人口/UNKNOWN | 证据边界 |
|---|---|---|---|---|
| LSTM `native-r3` | 451.09 / 520.91 | 7759248 / 7712068 | 4905 / 4911 / 6 | 源码提交 `9ce67d9a80ea4c3657f22f00e1beb1f0024fb00b`，两臂COMPLETE、10个源码内容身份全程不变；评分及顺序、成员、UNKNOWN、实际输入摘要逐项相同 |
| TCN `native-r2` | 502.30 / 464.99 | 7614588 / 7711912 | 4905 / 4911 / 6 | 两臂COMPLETE、同样五项相同；发生在最终文件身份护栏及context摘要修订前，只作早期源码诊断，不伪绑最终HEAD |

LSTM 实际模型窗口从2024-11-05开始，由原生 resolver 决定；没有截短为单日。`native-r2` LSTM 因运行期间源码审阅发生修改，被原生源码闭包校验拒绝；该次失败不计入通过，不覆盖。`native-r3` 单独在固定源码下重做。

磁盘：LSTM 两臂结束后各保留454155/457082字节；新入口运行中 `du -sk` 抽测1629540 KiB（约1.55 GiB，统计原生临时特征文件，不跟随大型模型链接）。这只是观测到的占用，**不是从启动到退出的完整峰值**，后续完整混合验收需补全程测量。没有复制大型模型或数据集。单次共享负载下耗时一升一降，不证明性能提升，也不能据此定性所有日的性能回归；未作冷/热重复分组性能验收。

独立草稿 PR #6356；源码提交 `9ce67d9a8` 的 CI run `39232982569` verdict通过。F2验证仍明确拒绝 F-336，CI绿不等于真实混合来源/原生TopK/停牌资格和LocalSIM接续验收通过。后续文档收据提交不改上述推理源码，也不把旧测量改绑新HEAD。

首轮缺口结论修正：新任务的政策已由全局统一 canonical 方向确定，不需要再问策略包 owner 选择政策，也不需要重训/改包。之前的字符串 equality 还混淆了合法 snapshot key 和 rolling key，这是本共享适配自身应修复的问题。历史旧政策两臂对照仅说明旧输入下的诊断结果，不代表 v18 验收。

## 接续审核与验证（R11 起）

同步主线 BUG-1986 / PR #6365，复用 `require_same_canonical_policy`、`require_canonical_new_task_binding`；不修改公共 PIT owner 源码。新增 RED→GREEN 证明同政策 frozen snapshot 被旧 equality 错拒；修复后保留 authority、参数 digest、发布 completeness 的拒绝条件。文件日以生产者已冻结成员文件按 C 解析原生 identity；显式旧声明不匹配时失败。数据库任务注册只读冻结原生活动身份，恢复不重查 active profile 或 live pointer。已有无来源计划的调用不改为本新合同。

56项直接相关测试通过（source-plan、CLI、kernel、batch adapter），涵盖新增文件身份继承、不访问 live pointer、错误日期/成员/hash、DB身份冻结和coverage漂移。不是跨模块全量验收。R4真实文件验证使用同日、同政策、同成员的原单源入口与新计划入口，对照原LSTM/TCN，不再用legacy人口作为canonical基线。

R11–R14期间只读运行状态：`DEPLOYED_LEGACY_PENDING_MIGRATION`，generation=0。当时新主线要求数据owner完成授权canonical迁移；R15该外部阻断已解除。不能制造generation/envelope或由本窗口写库/激活；此项与文件推理可独立验证。LocalSIM owner仍需把自身任务创建/session/provider/分钟读入接到公共计划，源码合入、后端用户重启、LocalSIM接续分别验收。

R13最终验证：另8项文件输入/推理只读测试通过，本轮直接矩阵合计64项；Ruff、diff-check通过。精确11文件ownership全部mapped；L0无阻断，1项既有P2字符串join提示已审阅。源码提交 `6a149833dbc0a7ea597ed51b2b9f32e1f8b94ee2` 已推送PR #6356，其HEAD的CI verdict SUCCESS；PR仍DRAFT，F2校验仍拒绝未完成的F-336，未合入或清理工作树。

真实R4使用活动v18冻结发布，C=2026-08-28、D/T=2026-08-31。两臂使用原模型权重、冻结processor和同一文件/PIT成员；所登记执行源码的内容身份全程不变。以下是推理级技术验证，不是原生TopK/资格/生产账户/收益验证。

| 模型 | 原单源入口秒 | 新计划入口秒 | 两臂峰值RSS KiB | 完整评分/人口/UNKNOWN |
|---|---:|---:|---|---|
| LSTM | 295.79 | 293.14 | 7840956 / 7849688 | 4917 / 4917 / 0 |
| TCN | 289.33 | 317.02 | 7882676 / 7903156 | 4917 / 4917 / 0 |

两种模型的完整评分及顺序、原始成员、UNKNOWN、实际输入摘要逐项相同。全运行期约10秒采样一次磁盘，占用观测最大值约1.52GiB；这是**采样最大值**，不是无遗漏的瞬时峰值。TCN新入口耗时较长且与小型恢复验证存在共享I/O负载，单次测量不声明速度提升或统计性能回归。没有复制大型权重或数据发布。

LSTM原生CLI `--resume` 在全新进程28.34秒完成，resource_loads=0；原scores.json与commit.json字节hash不变。收据 `native-r4/lstm-resume.json`。该收据只证明文件来源原生恢复；数据库重读摘要/漂移仍由定向测试验证，真实DB续段未放行。

接续顺序：①数据owner交付真实canonical活动binding/coverage（R15已完成，本窗口独立读回），不要求重建已有v18文件；②本窗口用新任务冻结同政策计划，验证文件最后日→数据库首日的完整历史窗口、原生TopK/资格及失败恢复；③LocalSIM owner接自身调度/分钟/provider/断点，复用公共计划而非另建选源算法。两端运行及用户重启状态独立报告，不能把文件推理通过替代这些步骤。

## R14 接续审阅范围（2026-10-20）

最新主线已同步。审阅发现CLI在计划来源为数据库时，每个模型请求都会新建session，与“每DB日新session、同日多模型复用已读输入”的既定设计不符。在已登记的 `scripts/strategy_package_live_inference.py` 和 `test_input_source_plan.py` 内增加定向RED→GREEN回归并修复：同日请求保留输入session，跨DB日必定重建，跨消费者/任务不共享。仅改变输入对象生命周期，不改变模型、原生历史窗口或任何选股算法。

两轮接续审阅：第一轮从三模式/多模型生命周期发现以上问题，两个新用例在原实现下均失败，修复后两个用例及原漂移恢复用例通过；第二轮检查跨DB日、文件跨日、部分失败、逐模型独立输出和精确恢复，不再发现需修改的缺陷。66项直接相关回归通过；测试替身只证明生命周期与拒绝合同，不替代真实数据库接续验收。

`native-r14/file-kernel.json`：读取R4已完成且逐文件hash核对的原生LSTM/TCN评分，交原生artifact准备、原生HMM关闭分支、唯一原生computation与实际 `SelectionSignalKernel.run`。两模型两臂均4917评分/4917人口/0 UNKNOWN，Top50、完整原始排序、排除项及成员状态逐项一致；原生停牌读取D=2026-08-31，risk时钟C=2026-08-28。行情与停牌来自同一冻结文件；数据库只读查询策略包身份，不读市场行情。使用预先明确的相同原生Top50政策（HMM/risk关闭、停牌过滤开启、行业黑名单为空），不修改任何既存任务政策。raw阶段沿用既有历史artifact的DB_HISTORICAL语义信封，物理文件来源在原生input context及来源收据中明确记录；没有伪称为数据库行情。本次6.13秒仅为既存评分到选股内核的验证耗时，不是模型推理耗时；不包含参考价加富、HMM开启、LocalSIM适配/生产验收或收益证明。

运行差异已复核：canonical root环境的实际目标为127.0.0.1:5432/aistock，后端ready且源码为主线 `2db54e36fb3997b6061c8f08acff5cbc19110238`，并非误连DEV。官方 `CanonicalPitAuthorityResolver.resolve_live_binding()` 仍读回 `DEPLOYED_LEGACY_PENDING_MIGRATION`，rule=`st_pub_next_trade_restore_active_l_v1`；新任务要求的政策为 `shsz_a_252td_st_delist_asof_v2`。因此“全局统一源码已实现”和“生产权威指针已激活”不是同一事实。本窗口不激活、不补写或改数据，不绕过公共PIT合同，不把旧政策当canonical运行。数据owner只需交付既有官方激活流程的有效binding及coverage读回；业务连续日、全模型历史窗口、过滤和恢复验证仍由本窗口完成，不外包给数据owner。现有v18文件不因此要求重建。

DESIGN-COMPLIANCE-001接续结论：①F-336保留BLOCKED，不把部分真实验证宣布为全量完成；②旧指针、不一致输入及漂移仍明确失败，不填值/回退/伪造状态；③原生模型、评分、过滤、递补、TopK和C/D/T不变，生命周期修复已同步设计；④没有新增策略准入、审批或人工门禁，仅遵守主线既有canonical身份合同。PR仍为草稿，源码合入、运行加载与LocalSIM接续均未验收。

R14最终静态结果：11个精确文件ownership全部映射；Ruff与diff-check通过，L0无阻断，既有P2命令字符串join提示不涉及数据合并且未改变。F2校验仍因F-336未完成而明确FAIL，不把扫描/单测通过等同于F2通过。结束前再次以只读事务读回：DB权威generation=0、物理key=`shsz_st_pit_active_v1`、旧rule不变，声明覆盖2018-08-01至2026-10-19；这不是“行情数据缺失”结论，更不要求重建数据，只说明活动政策未切到canonical。现有草稿PR保存当前进展，等待owner完成既有激活边界后继续真实混合验证。

## R15 canonical 跨界接续验收（2026-10-20）

owner PR #6412 已合入并激活后，本窗口独立只读确认 `aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2`、generation=1。同步主线 `e8fb5debe`，合并点 `100b0f430`；本轮推理期间全部12项执行源码内容hash不变，无模型权重、processor或原生算法变更。此前legacy状态及R1–R14测量仅保留为历史记录，不代表当前阻断，也不冒绑本轮收据。

官方冻结发布注册23.29秒，文件binding与R4完全相同，复用原不可变文件基线，不重新推理或覆盖旧产物。计划 `6a6672873e164db369d0cd384ea43e6be6ea9e799e389b48342f6e53791793f4` 的边界由发布覆盖及交易日历解析：D/T=2026-08-31、C=08-28读文件；D/T=2026-09-01、C=08-31读数据库。两个真实包的manifest另与只读注册记录核对通过；包与模型名称仅是验收样本，不进入生产路由条件。

| 模型/日期 | 实际来源 | 完整评分/人口/UNKNOWN | 原生历史起点 | 与同源单源基线 |
|---|---|---|---|---|
| LSTM / 08-31 | 文件 | 4917 / 4917 / 0 | 2025-07-02 | 完整评分、顺序、成员、UNKNOWN、实际输入摘要一致 |
| TCN / 08-31 | 文件 | 4917 / 4917 / 0 | 2025-07-02 | 同上，全部一致 |
| LSTM / 09-01 | 数据库 | 4911 / 4911 / 0 | 2025-07-03 | 同上，全部一致 |
| TCN / 09-01 | 数据库 | 4911 / 4911 / 0 | 2025-07-03 | 同上，全部一致 |

本轮数据库基线是同政策新进程原单源CLI；计划臂是在同一CLI进程中连续执行两日×两模型。首DB日完整原生窗口全部走既有DB reader，未拼接文件尾部，未缩短历史。文件与数据库是不同日期、不同人口，未要求两日数值相等；比较对象始终是同日期/同政策/同来源的基线。

| 原生执行臂 | 请求数 | 端到端秒 | 真实进程峰值RSS KiB | 10秒采样磁盘最大KiB | 完成后产物字节 |
|---|---:|---:|---:|---:|---:|
| 新数据库单源基线 | 2 | 602.81 | 9928312 | 1645016 | 846049 |
| 文件→数据库计划 | 4 | 1071.47 | 9277744 | 1974232 | 1690882 |

两臂请求量不同且共享机器负载不同，以上是功能验收资源记录，不是冷/热缓存控制实验，不据此声明提速或性能回归。RSS来自原生`/usr/bin/time -v`，磁盘只是全程10秒采样的最大值，不是无遗漏瞬时峰值。模型/发布均原地只读复用，无大型资产复制，Windows及WSL临时路径全部X盘对应目录。

`native-r15/kernel/receipt.json`：hash核对原生评分后，两模型×两日×两臂均实际进入原生artifact准备及`SelectionSignalKernel.run`，无持久化repository。完整原始顺序、Top50、UNKNOWN、成员及排除项全部一致；分别用原生参考价loader核对50只入选股，两臂真实价格及观测日一致、均不晚于C。停牌资格读取D，risk调用时钟C。政策显式为Top50、停牌过滤开启、HMM/risk关闭、行业黑名单空；不更改已有任务，不声称覆盖HMM开启场景。18.01秒是复用已验证评分的选股/参考价合同核对耗时，不是模型推理耗时。

`native-r15/retry.json`：新进程以同一run、计划、代码和模型执行原生`--resume`，文件段重新核对冻结身份，数据库段重读完整原生窗口并逐项比对实际输入与评分。结果COMPLETE/PASS，977.34秒，峰值RSS9134428KiB；4份原scores与4份commit字节hash全部不变，未覆盖旧产物。DB恢复的实际重读成本是缺少不可变DB revision时的明确限制，不能据一次测量推导通用每日速度。

R15第三轮交付复核还修正了接入示例中的Python调用语法，示例AST解析通过；没有改变数值源码。源码合入、后端运行加载、LocalSIM私有接入和生产接续仍是独立状态。本交付是可显式调用的新模式，不迁移旧任务，不包含业务API默认切换、LocalSIM账户或分钟行情适配，不提供模型收益证明。

交付前再次同步主线`0f66abdcd`（仅继承owner月度probe及HMM训练资产登记修订），12项本次执行源码hash仍与真实验收一致。66项直接回归再次全部通过（12.53秒），F2校验7项/7行全部PASS、0警告；DESIGN-COMPLIANCE-001四项及精确11文件范围复核通过。后续PR新HEAD须完成自己的CI，历史绿灯不替代最终检查。
