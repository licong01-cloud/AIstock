# LocalSIM 独立期间账户回放 F2 设计

日期：2026-11-06；风险T3；feature tier F2。承接已合入#7048的输入契约和#7054的最终价集/期间分析。此阶段交付真实10月账户事实和一次经济exact retry，不将其解释为策略有效。

## Background / 背景

v2准备已冻结17日850个原生目标和13户9月末实际状态。原v1研究入口从1000万元空账户启动，不能冒充期间续接。择时owner已交付3174条模板及公开`PeriodAnalysisSpec`合同，无需等待QE、行情或服务重启。

## Scope / 写入范围

- `backend/services/simulation_execution/localsim/period_research.py`：新增版本化freeze/run/inspect/economic-retry及facts/spec输出。
- `backend/services/simulation_execution/localsim/dual_mode_research.py`：提取公共账户日编排，复用原账户计算与提交；旧入口默认业务和经济定义不变。源码指纹按正式合同随代码变化；不在新版代码下恢复旧源码研究。
- `backend/services/simulation_execution/localsim/timing_campaign.py`：仅v2库存/价集消费的历史持仓种子及资源关闭接缝；旧v1默认语义不变。
- `backend/services/simulation_execution/localsim/timing.py`、`human_portfolio.py`、`native_research.py`：修复审核发现的历史取消事件时钟/随机ID问题。仅显式传入历史confirmed_at的离线调用生成确定性取消反馈；未传入时旧默认不变。不忽略经济event_time或event_id，不修改通用OMS和择时核心。
- `backend/tests/simulation_execution/test_localsim_period_research.py`：直接合同、恢复、经济重算和兼容测试。
- `backend/tests/simulation_execution/test_localsim_historical_cancel_clock.py`：取消时钟、确定性、重试及默认兼容定向回归。
- 本设计及`docs/analysis/localsim_period_account_replay_acceptance_20261106.md`。

临时计划、脚本、审计和运行产物位于`X:/AIstock_temp/localsim-october-account-replay-20261106`，WSL直接消费/mnt/x、/mnt/f。独立工作树从最新origin/main建立。不修改择时、Selection、Advisory、QE、HMM、数据准备、数据库、数据集、服务或生产账户。

审核发现的原生OMS历史事件时钟问题已登记BUG-2186 / Issue #7065，并由独立修复PR #7068交付。对应BUG JSON仅记录自身修复范围和既有运行核验引用；本期间功能不新增公共目录豁免。离线新进程可加载已验证源码执行研究；共享方法的后端激活仍按原目录保留用户重启门槛，两者独立。

## Non-goals / 非目标

不重跑1—9月、不重训、不重新推理、不调参数/费用、不充值或清空持仓、不改旧288场景、不复制价格公式或收益/回撤比较器。原TWAP默认保留。不得用价集retry、缓存命中、账本回读或CI代替本轮正式账户经济重算验证。经济归因由择时窗口消费正式事实后完成。

## Architecture / 架构

v2准备+最终模板/manifest → 唯一共享来源检查 → 独立期间request → 公共LocalSIM账户日编排 → 原生journal/day/results → 最终来源/结果绑定receipt → facts/PeriodAnalysisSpec。失败仅保留完整前缀；每个新研究有独立run/cache/output/断点。

继续调用唯一`_compute_account_day`、原生TWAP/人工账户、费用、`commit_portfolio_day`、`TimingFileRepository`及`CandidateReader`有界预取。公共编排参数化初态和日schema；旧v1保留空初态及原统计，v2不调用旧v1空账户验证器。

离线原生适配器显式启用历史事件能力：成交事件沿用已确认fill.trade_time；取消沿用人工派发/到期/共同终点的实际确认时刻，取消ID由run、order、reason及该时刻确定生成。通用OMS、成交逻辑、数量和费用不改；不通过删除event_time/event_id实现重试相等。原生TradingRuleDecision依既有模型验证decision_hash，再用原生事实序列化排除其系统created_at。

## Contracts / 合同

`localsim_period_study_v2`绑定preflight、最终templates/policy/manifest、producer commit、LF源码指纹、精确17日、FULL_13初态引用、费用文件SHA及执行合同canonical SHA。资源参数batch-days/account-workers/optimized不进入经济身份。

初态account_id沿用v2 opening，与父账户一对一。实际cash/positions/avg_cost/available quantity继承；original 1000万元只是资本原点。first-day marks采用已冻结opening marks；每个交易日经原生T+1推进。按日引用已有projection，不推理或跨日复用资格/计划。

开跑前完成全部原始source检查并发布`raw_source_inspection.json`，然后才读取任何本期间收益或既有day。恢复仍先检查来源；已确认journal只能在现金/持仓/T+1、前序FileRef及身份一致时复用。日提交继续单写者原子事务，未确认/损坏前缀拒绝，读取已提交day不会重复成交。

经济exact retry单独显式调用一次：同一个冻结request与经济身份，从相同初态重新调用账户日计算，逐日比较真实账户及经济facts；与已提交journal不等即失败并保留证据，不能覆盖旧day或重复提交成交。父订单审计中的order/fill/event/state先按各原生模型验证，再调用既有`local_sim_fact_payload`排除模型自身的created_at/updated_at；state_hash先由原生模型验证，比较域中改用canonical state fact摘要，避免把已排除的进程创建时刻通过派生hash重新带入。原始审计及hash全部保留；不删除因果时间、行情hash、价格/数量/费用/NAV或反馈，不增加数值容差。状态`EXACT_RETRY_VERIFIED`必须伴随完整221账户日的重算与经济相等证据；普通run/inspect不自动触发重算。此为经济事实相等，不声称运行审计时间戳逐字节相等。

results仅封装本期间真实day、终态和执行事实，不自建收益比较。所有13户17日未成交情况保留，全部NAV观察可回读。最终`source_inspection`在完成后绑定原始检查FileRef、preflight/templates/candidate、日期、producer、account_days_sha256、费用与执行合同SHA；不要求运行前预知结果hash。

facts严格使用`position_timing_period_facts_v1`，spec由公开`PeriodAnalysisSpec`构造。facts→最终source receipt→结果/原始source/day引用有向无环，最后发布交付receipt；全部完整SHA可回读。只调用公共期间source验证验兼容，不在本窗口运行真实收益分析。共同终点14:57，TTL90秒、主要延迟60秒、三规则×四延迟与native基线、费用及工程价格政策不变。

## Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-401 | v2准备及最终价集/源码/费用/执行身份闭合，来源先于收益 |
| F-402 | 13户承接实际初态，原生T+1、资金/成本/持仓及旧TWAP不变 |
| F-403 | 公共账户日计算/协调器复用，有界预取及并行资源与结果分离 |
| F-404 | 部分前缀恢复/幂等无重复成交，损坏/漂移fail-closed |
| F-405 | 显式一次真实经济exact retry，完整账户日比较且旧产物不可变 |
| F-406 | 正式results、完整NAV facts、最终source receipt及公开spec可回读 |
| F-407 | WSL真实221账户日、inspect/经济retry及不操作DB/服务/旧研究 |
| F-408 | 多轮审核、定向回归、F2/L0、PR/CI与交回owner明确分列 |

## Implementation Plan

先记录合入回执和冻结范围；补共享编排/v2接缝；实现期间request/结果/facts/spec/显式经济retry；小型真实账户夹具验证及多轮审核修复；冻结代码后在WSL运行全部221账户日，再inspect及一次经济retry；交付绝对路径和完整SHA；通过本地门禁/CI后按已有授权合入。收益分析交回择时窗口，不等待重启。

## Verification Plan

验证旧v1默认结果、历史持仓SELL、first-day开账/T+1、原生费用和本金、全人口、source-before-outcomes、坏SHA/变政策、部分提交与恢复、不重复写入、重算实际调用/差异拒绝、不同资源配置身份不变、完整NAV事实及公开period_source_preflight。真实WSL直接消费冻结文件；没有经济收益时不填0或假结果。失败后先跑失败node；稳定后只跑一次相关小矩阵；广回归交CI。

## Risks / Rollout / Rollback

账户研究为回顾性离线验证，不证明严格前瞻或超额收益。独立身份/目录，无生产账户激活。源码可回滚而旧成果不删；新source变动不能静默恢复旧request。原始source审计与最终消费者receipt分开，避免先有结果hash才能开跑的循环依赖。

## Production Gates

DDL=noop；database/dataset/market-network/service/production-account操作为false。无需后端重启作为离线前置，线上加载未请求且未验收。源合入、离线加载、账户回放、经济retry及经济效果分别报告。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-401 | period_research.py；timing_campaign.py | 定向身份/来源顺序测试；冻结输入收据 | planned | none |
| F-402 | dual_mode_research.py；period_research.py | 原生账户日及T+1/资金继承测试 | planned | none |
| F-403 | dual_mode_research.py | 复用账户池与有界Reader测试 | planned | none |
| F-404 | dual_mode_research.py；period_research.py | 部分提交/恢复/坏前缀测试 | planned | none |
| F-405 | dual_mode_research.py；period_research.py | 实际重算计数、事实比较、不可变产物 | planned | none |
| F-406 | period_research.py | 公开PeriodAnalysisSpec及来源验证 | planned | none |
| F-407 | 本设计及验收报告 | WSL 221日、inspect和经济retry | planned | none |
| F-408 | test_localsim_period_research.py；验收报告 | F2/L0、定向矩阵、最终PR CI | planned | none |
