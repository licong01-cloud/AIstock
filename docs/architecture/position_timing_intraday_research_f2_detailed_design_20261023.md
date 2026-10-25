# 分钟择时历史研究与比较能力 F2 详细设计

日期：2026-10-23；验收更新：2026-10-24；状态：TIMING_RESEARCH_IMPLEMENTED_VERIFIED。
上位蓝图：[分钟择时蓝图](position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)。
源码起点：267198349d766c6c39a69d037f9513fc580122d7。

## Background / Scope / Non-goals

用户要求一次连续任务完成后续设计、开发及具备条件的验证，后端重启由用户执行。
本轮目标是回答同一既定交易计划下，分钟规则相对简单执行是否改善成交与账户结果。
先完成研究能力，再集中执行历史验证；功能与因果检查随开发完成。
已有 B0/C1/C2 和人工提醒保持原参数，日频模型/股票筛选不在本轮改变。

择时窗口负责冻结文件输入、多日规则矩阵、决策回放、LocalSIM 消费包及结果比较。
LocalSIM 仍有 TWAP-only 上位限制；本窗口不改其代码，不创建第二账本，不自行把机会记成成交。
账户级费用、成交、终值和 MDD 由其 owner 提供绑定本研究身份的结果文件。
缺消费者结果时报告 NOT_RUN_LOCALSIM_ADAPTER_PENDING，继续交付和验证本模块，不能把空结果当零收益。
无实时行情、数据库、生产 API 写入、训练平台、调度器或跨模块修改。

## Architecture

新增三个离线文件，复用 intraday/core.py 的唯一规则实现和 replay.py 的前缀/延迟语义：

| 文件 | 职责 |
|---|---|
| intraday/research_inputs.py | 显式 candidate 与计划文件转换；逐消费文件核对 producer SHA；raw 分钟、交易日、日级事实 |
| intraday/research.py | prepare/run/inspect/exact retry；逐日逐策略独立状态；冻结消费包、诊断与完整人口 |
| intraday/research_results.py | 接收 LocalSIM 显式结果，严格配对和汇总；不实现成交/账本 |

新增 backend/tests/position_timing/test_intraday_research.py 与 test_intraday_research_results.py。
文档只更新本文、分钟总蓝图、LocalSIM交接包与本轮实施记录。
状态与产物仅在既有 artifact root 的 intraday_v1/research_studies 下；不改线上策略或全局 registry。

## Contracts

### 输入与人口

研究 spec 绑定 candidate manifest 的绝对路径/文件 SHA、计划文件路径/SHA、明确源码 commit、费用合同路径/SHA。
计划沿用 PlanEvent：方向、数量、价集、available_at、有效期、revision 均来自冻结文件。
研究复制同一原始计划形成 B0/C1/C2 三臂，原计划身份与派生计划身份分别保留。
不能从回放收益选择股票、日期、方向或价集；没有价集的 C1 保留 NOT_APPLICABLE_PRICE_CONSTRAINT，不能补无限区间。
原始计划允许取消/修订；每个原 plan_id 只能属于一个股票/交易日，revision 严格递增。
研究首轮 allow_split=false，隔离完整计划的一次执行时机；分批反馈闭环仍由 LocalSIM 接入后评价，不能用 HUMAN_UNKNOWN 推进模拟批次。

输入支持已有冻结 MarketView 文件以及只读 Qlib candidate 转换。
转换使用 components/minute_bin_candidate 和 daily_bin_candidate，清单按 manifest 中的消费文件逐项核对路径、size、SHA。
日历与 instrument spans 独立校验；provider catalog 不是选股依据。
adjusted OHLC / factor 恢复 raw CNY；volume × factor 恢复股，amount 原样保留 CNY；复用既有 raw_price_li 精度转换。
停牌来自冻结 suspend_d，并复用已有纯投影实现；已确认整日/盘中停牌保留 WAIT，不填价格/股份。
缺 raw limit/pre_close、无解释缺bar、factor非正/非有限、缺源文件或哈希漂移明确失败。
回放只使用连续竞价时段已闭合分钟，排除集合竞价；不将缺分钟伪造为零成交。
历史 available_at 默认规则显式冻结为 bar end+5秒，轮询分钟+10秒，标 ASSUMED_VISIBILITY，非真实网络时延证据。
日级事实09:00可见是本轮历史研究假设；不宣称已取得其原始发布日志。

### 时间、三规则及四档延迟

B0 FIRST_OBSERVABLE_V1、C1 PRICE_TOUCH_V1、C2 BOUNDED_REVERSION_V1 参数沿用阶段一冻结值。
每个交易日从09:30至14:57回放；截至14:57停止产生本轮连续竞价机会。
D0/60/180/300秒全量保留，D1为主要产品观察口径。
NEXT_ELIGIBLE_BAR_OPEN 的执行bar起点严格晚于提醒可见时刻+人工延迟；既有午休、截止及修订取消语义保持。
输出 EXECUTION_SLOT_ELIGIBLE 仅表示机会与时钟条件，不能声称成交或用信号bar close算收益。
整个研究进入收益比较前先完整核验所有输入和所有决策子包，不能只审计盈利/可成交子集。

### 不可变研究身份

prepare 完成全源 preflight，冻结 candidate、全部计划、市场、成本、参数、源码文件 SHA 和原始人口。
不同日期/规则为独立可复用子request；run逐个调用既有回放，汇总诊断并生成 LocalSIM consumption package。
source preflight 可以读取市场价格以校验，outcomes_read=false 指尚未读取账户结果或计算收益。
inspect 重新校验 source/request/子包/汇总/manifest；exact retry 同输入返回 ALREADY_MATERIALIZED，旧产物不覆盖。
新增研究源码和复用源码的身份都加入研究request；Python源码仅归一化LF，数据仍使用原始字节SHA。

### LocalSIM结果接收与比较

结果文件须声明 schema、producer=LOCALSIM、producer_source_commit、release_identity、研究bundle manifest SHA、cost-policy SHA、execution-contract SHA。
每个原计划 × 适用规则 × 四档延迟必须且只能有一条结果；无机会/无成交也必须有行。
结果行携带原计划身份、初始账户hash、初始净值、目标数量、成交数量、成交均价（未成交为null）、费用、共同终止时刻/终值、MDD、等待及typed结果状态。
同计划的全部规则及四档延迟必须有相同初始账户、初始净值、目标数量/金额、共同终止时刻及费用/执行合同；净结果仍与同延迟B0配对。
未完成计划仍有终值，包含原现金/剩余持仓；UNAVAILABLE 行保留分母，不能填0终值。
读取外部结果前必须 inspect 整个研究成功；结果文件先校验SHA再解析，记录 outcomes_read_stage=AFTER_COMPLETE_SOURCE_AND_BUNDLE_INSPECTION。
价差仅在双方均有成交时报告并披露配对数量；买方 (baseline_price-candidate_price)/baseline_price，卖方相反。
账户净增量=(candidate_terminal-baseline_terminal)/initial_nav；MDD差异由两行已注明定义的账户MDD比较，不把单笔价差冒充MDD。
报告 BUY/SELL 和 ALL、每规则/每延迟的原始计划数、适用/不可用/已成交/全部完成数、配对数量、等权和目标金额加权改善。
不要求正收益才允许报告；本轮输出为描述性证据，不自动晋升策略、调参或宣称统计显著。
native TWAP结果可另作明确命名的产品对照，不能把B0重命名为native TWAP；本轮强制主比较为matched B0。

## Implementation Plan

同一开发包内顺序为：冻结设计及写入边界 → 输入/批研究/结果比较实现 → 定向功能与因果验证、多轮审核 → 历史文件真实输入回放 → CI/提交合入/清理。
工程验证人口预先固定：000001.SZ、600000.SH、688981.SH，2026-09-21至2026-09-24每交易日BUY/SELL各一计划，数量1000股。
价格偏好固定为买入[0.995×pre_close, pre_close]、卖出[pre_close,1.005×pre_close]，端点按0.01元外扩；不是训练所得或生产荐股。
该人口用于真实数据接线与决策机制验证，不代表全市场或实际部署人口。任何数据缺口不删股/缩期，只记录并交数据owner。
candidate显式选择当前本地已存在的20260930月度minute1修正版，身份在prepare前冻结；不修改active profile。
可用WSL Python运行离线CLI；小矩阵按日期串行复用，先保证可重复，未启动全市场耗时搜索。
若LocalSIM尚未交付结果，完成择时侧开发、真实决策回放和可复制需求包；账户经济评价如实保持未执行。

## Verification Plan / Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-001 | 全源及candidate身份先于收益；raw/factor/单位/PIT范围正确 |
| F-002 | 三规则同人口，C1不适用保留，四档延迟与取消语义一致 |
| F-003 | 多日独立状态，完整人口/不可用透明，未来信息不进入决策 |
| F-004 | request/子包/消费包/结果哈希闭合，inspect/exact retry不可变 |
| F-005 | LocalSIM结果配对校验、费用/账户/时点一致，全计划分母 |
| F-006 | 真实数据与fixture证据分开，输出不冒充收益/成交，owner边界明确 |

功能测试只覆盖有业务风险的身份/因果/缺失/配对/分母，不堆叠内部结构快照。
两轮以上审核分别覆盖数据与因果、经济口径与交付边界，发生修复后补相关回归。

## Rollout / Rollback / Risks / Production Gates

本轮为新增离线研究CLI，无路由/线上默认策略变化，不要求后端重启。
发布新的request与bundle，不覆盖旧研究；撤回使用只停止引用新request。
LocalSIM消费者未接入是账户研究的真实依赖，不阻塞择时侧功能交付；未执行经济验证必须可见。
风险为历史可见时间假设、价格偏好机制人口与真实计划分布差异、模拟执行与真实成交差异；报告不外推盈利。
database_read/write=false、live_market/network_data=false、process_control=false、runtime_action=false。

## Design Acceptance Matrix

下表验收仅覆盖本轮择时侧研究能力；LocalSIM真实账户结果生成/产品接入不在本窗口实施范围，仍按上位蓝图单列未完成。验证及真实回放证据见本轮实施记录，不用fixture证明真实经济收益。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | intraday/research_inputs.py::CandidateReader；research.py::_request | backend/tests/position_timing/test_intraday_research.py；artifact:docs/analysis/position_timing_intraday_research_implementation_20261023.md | VERIFIED | none |
| F-002 | intraday/research.py::prepare/_outputs；replay.py::simulate | backend/tests/position_timing/test_intraday_research.py；backend/tests/position_timing/test_intraday_replay.py | VERIFIED | none |
| F-003 | intraday/research.py::_inputs/run；inputs.py::visible_prefix | backend/tests/position_timing/test_intraday_replay.py；artifact:docs/analysis/position_timing_intraday_research_implementation_20261023.md | VERIFIED | none |
| F-004 | intraday/research.py::inspect/run；research_results.py::compare | backend/tests/position_timing/test_intraday_research.py；backend/tests/position_timing/test_intraday_research_results.py | VERIFIED | none |
| F-005 | intraday/research_results.py::EconomicResults/summarize/compare | backend/tests/position_timing/test_intraday_research_results.py | VERIFIED | none |
| F-006 | research.py::_outputs；research_results.py::compare；LocalSIM交接包 | artifact:docs/analysis/position_timing_intraday_research_implementation_20261023.md | VERIFIED | none |
