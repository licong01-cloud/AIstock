# 分钟择时历史研究能力：实施与验收记录

更新：2026-10-24。范围：[研究F2详细设计](../architecture/position_timing_intraday_research_f2_detailed_design_20261023.md)。

## 交付与未完成项

已实现择时侧多日历史研究、三规则四延迟矩阵、不可变消费包、LocalSIM结果校验及配对比较；没有新增账户/撮合系统、在线API、模型训练、调参、调度器或跨模块修改。
真实冻结文件回放已完成prepare/run/inspect/exact retry；重复prepare返回相同request，重复run返回ALREADY_MATERIALIZED、integrity=VERIFIED，manifest身份不变。
LocalSIM仍为TWAP-only，真实账户结果尚未提供，故`economic_evaluation=NOT_RUN_LOCALSIM_ADAPTER_PENDING`、`outcomes_read=false`。不能宣称策略提高收益或降低账户回撤。
阶段一核心/API已于PR #6611合入且用户重启后验证；本轮新增的三个离线文件不要求再次重启。

## 实现清单与研究身份

源码实现commit：`06e375e0edbdd3bd503bdaebf291eef8dbb47545`（含首批实现`9d8d7b5f5a35b28bd565ad5b830f1952c5f86b67`及审核修订）。后续文档提交不改变研究源码文件hash。

| 位置 | 交付 |
|---|---|
| backend/services/position_timing/intraday/research_inputs.py | 显式candidate、消费文件SHA/size、日历/bin边界、raw分钟与冻结停牌投影 |
| backend/services/position_timing/intraday/research.py | 多日prepare/run/inspect、B0/C1/C2×D0/1/3/5、人口与子包闭合 |
| backend/services/position_timing/intraday/research_results.py | 完整人口/账户/费用/执行/终值匹配；买卖价差、净增量、MDD降低与分母 |
| 两个test_intraday_research*.py | 数据/配对/缺失/重试的业务断言，无重复内部快照 |

artifact根：`F:\Dev\AIstock_model_artifacts\position_timing_advice_v1\intraday_v1\research_studies`，WSL为同路径`/mnt/f/...`。源码从任务独立worktree执行，环境为Ubuntu的`/home/lc999/miniconda3/envs/rdagent/bin/python`，没有安装或修改环境。

| 对象 | 不可变身份 |
|---|---|
| candidate | `X:\AIstock_dataset_candidates\backtest_dataset_candidates\20260930-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4ab0b48642c3-margin1-minute1` |
| candidate qe_dataset_manifest.json完整文件SHA | `f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed` |
| spec相对路径 | `operator_inputs/20261024_real_smoke_v2/spec.json` |
| spec完整文件SHA | `c836eb4b596fca5a09b208350e2e68f11f7d042da99eebc4a6f3ee77f8c96e66` |
| 人口provenance完整文件SHA | `6c24fb9dd82f41e41cbcc7cfb7d85eb1eb0ec3590dfbf360e52fd4873cf59785` |
| request canonical SHA / bundle目录名 | `1c684e78b94872878608cafc48a7a496540b9a9c8cdf62424d886154398d2062` |
| bundle manifest canonical SHA | `8b9128a33c15464c0784b953d658d0666fa2697608e46b073f3a3691ae81b9c1` |

request位于`requests/<身份>.json`，bundle位于`bundles/<身份>`。不可变输出使用`{sha256,payload}`信封，canonical SHA不等于含信封的文件SHA；spec/plans/cost输入则是普通JSON文件SHA。源码身份为LF归一化Python文件hash及原字节JSON authority hash。
审核前的request `309b9ebd347bfd361f5ae47bd153f830482512d36231b0d7276c635f28ab0954`仅prepare，未读取收益；修订代码后新建身份，保留旧产物而不覆盖。v2人口没有换股/日期/参数，仅绑定审核后源码。

## 真实数据验证人口与结果

预声明000001.SZ、600000.SH、688981.SH；2026-09-21/22/23/24；每天每股BUY/SELL各1000股目标，共24原计划。价格偏好为pre_close附近0.5%区间，非训练预测/生产荐股。1000股不是账户资金或可卖量证明。
读回34个producer-pinned组件；12个股票日各237根连续竞价已闭合分钟，共2844根，排除尾盘集合竞价；没有删股、缩期、补数据或写candidate。
日级事实09:00可见、分钟end+5秒、分钟+10秒轮询为冻结研究假设，不是历史实际网络观测。物理instrument span仅检查行情覆盖，不冒充历史PIT选股；本研究人口是显式既定计划。

完成12个日期×规则子研究，288个适用case；23,544条逐时决策、67个机会、268条延迟意图。没有机会也保留case及原始人口分母。

| 规则 | D0可执行槽计划数 | D1 | D3 | D5 | 各列原计划数 |
|---|---:|---:|---:|---:|---:|
| B0 FIRST_OBSERVABLE_V1 | 24 | 24 | 24 | 24 | 24 |
| C1 PRICE_TOUCH_V1 | 19 | 13 | 7 | 2 | 24 |
| C2 BOUNDED_REVERSION_V1 | 24 | 18 | 18 | 18 | 24 |

这里“可执行槽”只表示时间与机会仍有效，**不是成交率**。C1有5计划始终没有机会；其余19机会中D1/D3/D5分别有6/12/17个在等待后失效。C2有6机会在D1及更长延迟下失效。研究没有模拟这些计划的现金/持仓终值，不能把失效当零收益，也不能仅凭此表判定B0更赚钱。
初步机制发现：价格触发对人工响应延迟敏感，应优先取得全计划账户结果，再决定是否研究机会持续性/条件失效机制；本轮不据此回选参数或换策略。

## 验证、多轮审核与设计符合性

1. 第一轮（合同/人口）：确认保留三臂全人口、无价集C1显式不适用、分钟raw恢复、独立日期状态、同延迟B0、未成交仍须终值；修复Qlib instrument tab分隔与日期内空格误读。
2. 第二轮（因果/完整性）：复核整批source inspect先于账户结果；修复bin超日历与倒置限价遗漏；同计划四档延迟也绑定相同初始账户，防止改变资金造成虚假差异。
3. 第三轮（真实输入/交付边界）：WSL完整历史回放与独立inspect，核对34源组件/12子包/288case和未成交分母；确认只有研究结果接收器而无LocalSIM账本替代物，更新蓝图及owner交接。

定向测试命令：`rtk pytest backend/tests/position_timing/test_intraday_research.py backend/tests/position_timing/test_intraday_research_results.py backend/tests/position_timing/test_intraday_replay.py -q -p no:cacheprovider`，结果14 passed。既有回放测试包含未来扰动/取消/边界，新测试包含raw价格与股数、非法因子/限价/bin边界、全人口重试、手算买卖价差与净结果、缺失/重复/错配/跨延迟账户、先审计再读收益。
手算比较结果仅是带`FUNCTIONAL_FIXTURE`标记的功能证据，不是上述真实研究的经济结果。
五个Python文件Ruff通过；L0成功，quality guardrail 0 finding，repo guardrail 1 finding/0 blocking；最终九文件ownership 9 mapped/0 unmapped/0 ambiguous。研究详细设计F2为6条/6行/0警告，蓝图F2为12条/12行/0警告，均PASS；CI运行结果由关联PR记录。
classifier：`targeted_ci_required`，`backend_sessions=[position_timing_backend]`，`workflow_gate=passed`，`unexecuted_test_files=[]`，`unmapped_code_files=[]`，无DEV DB/前端任务。

DESIGN-COMPLIANCE-001逐项结论：

- 禁止简化交付：本轮六条F-ID均有代码/测试或真实文件证据；明确未接入LocalSIM/Advisory和未完成账户经济评价，未冒称整体双消费者交付。
- 禁止静默错误：缺源/漂移/非法数据/不配对显式失败；无机会/不适用/UNAVAILABLE保留分母。
- 禁止改变业务语义：原B0/C1/C2核心、在线默认参数、日频策略、费用默认及其他模块代码均不变。
- 禁止私增门禁：无审批/MDE/正收益/未来自然日准入；必要源身份与配对校验服务于可解释比较，基础交付不等待账户结果。

## 后续与操作状态

下一步由用户将[LocalSIM接入包](../handoff/position_timing_intraday_localsim_integration_request_20261023.md)转交其owner，提供初始模拟账户及全288case结果。择时比较器已能在完整inspect后读取结果，输出净增量、条件价差、MDD降低、完成率/等待和完整分母；账户结果不足时不推进有效性结论。
本轮`account_economics_simulated=false; orders_created=false; database_read=false; database_write=false; network_data_accessed=false; dataset_mutation=false; runtime_action=false; process_control=false; backend_restart_required=false; production_ddl_gate=noop`。Git/CI仓库网络不等于研究读取行情网络。真实样本只是工程机制人口，模型与收益有效性仍需后续有界研究。
