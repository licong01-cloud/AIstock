# 精确LSTM重排序真实前向闭环验收

2026-11-02接续校正：下方11月1日的实时状态只记录当时事实，不是开发等待条件。程序开发、回归和合入改由[历史工程回放F1](../architecture/advisory_lstm_historical_engineering_replay_v1_f1_design_20261102.md)独立完成：原三折126D/6300行真实回放、当前公共计算逐项对账、现有页面消费真实历史样本，并显式隔离模拟时钟与实时行情。该工程路径不启动生产捕获、不修改原V2十文件闭包；正式150D仍沿自然调度观察，成熟收益及唯一经济判断另行验收。不能用历史已消费开发数据替代正式确认，也不再要求等11月6日/9日才验证页面或计算。结果与精确证据见[本轮报告](../analysis/advisory_lstm_historical_engineering_replay_20261102.md)。

2026-11-01。本轮执行数据晚到、跨日恢复、输入版本与幂等验收。结论为：**当前可核对的真实消费链路和新增连续场景通过；未来正式捕获、成熟收益与经济价值仍待真实发生。** 未发现需要修改冻结生产源码的缺陷，故只补充本运行说明和连续场景回归，不新增后台任务、观察页或模型。

依据：[原业务合同与V2设计](../architecture/advisory_lstm_top10_business_review_v1_f2_design_20261101.md)、[旧开发价值诊断](../analysis/advisory_lstm_rank_value_review_20261101.md)。既有100bps/5TD净增量目标、同一Top50内双Top10、十槽等权、费用、UNKNOWN/CASH和生产Top20边界保持。没有新F0/F1/F2业务功能或运行时行为变更。

## 当前验收结论与证据

证据根：`X:/AIstock_temp/ADVISORY/lstm-forward-acceptance-20261101`。`live-input-readback-20261101.json`记录真实只读复核，`validation_receipt.json`记录本轮回归和审核；交付/PR/CI以`delivery_receipt.json`为准。

真实复核重新调用既有`LstmSpoShadowSource.inputs(publication)`及`verify_capture(receipt)`，对象是已经捕获的D=2026-10-30先行批次，不是未来11月2日输入。五个数据库会话全部为`REPEATABLE READ / READ ONLY`，45次SELECT及45次本地会话超时SET，没有数据库写入。未调用fit、finance、capture或run_once，未产生新预测、收益标签或正式经济判断。

| 验收项 | 真实结果 | 结论边界 |
| --- | --- | --- |
| 原名单与分数来源 | 原50只顺序保持，原唯一分数资产及消费分数校验通过 | 未重建或筛选研究人口 |
| 当前特征 | 八字段均无缺失；D对应20TD为2026-09-28～10-30 | 不证明未来D就绪或历史PIT成立 |
| 实际依赖版本 | 40份行情/复权分区证明；沪深300 close 20/20有效，计算后同范围复核通过 | 不把其他指数的负面状态改成通过 |
| 共享就绪标识 | consumer_ready=true、prediction_ready=true、all_sync_complete=false、0 blocker | 依赖已就绪可消费；全局未完成事实保留 |
| V2健康 | CAPTURE_VIABLE_SO_FAR；正式捕获0；已知44个预定D，后续106个待官方日历，已到期捕获缺口0 | 页面先行捕获1与正式研究捕获0口径不同 |
| 不可变来源 | 原模型、release、先行预测、历史报告及V1/V2协议物理hash不变 | 没有新拟合或替换旧预测 |

数据仍为`CURRENT_DATABASE_NON_VINTAGE`。本次复核只说明当前消费和版本绑定正确，不追认历史首次可得时间。不能用该先行批次当前输入READY推定正式首D已经READY。

## 捕获和成熟时钟

以下来自已冻结的官方日历；所有时刻均为上海时间。

| 批次 | 决策D | 目标T | 固定5TD终点H | 2026-11-01状态 |
| --- | --- | --- | --- | --- |
| 先行批次，不计正式150D | 2026-10-30 | 2026-11-02 | 2026-11-06收盘 | 已捕获、未成熟 |
| 首个正式批次 | 2026-11-02 | 2026-11-03 | 2026-11-09收盘 | 尚未到D，未捕获 |

数据消费由实际依赖的就绪标识和当前revision驱动，不设“21点即默认完成”。原调度配置为收盘后16:30起每300秒检查；数据晚到时可返回WAITING_DATA并继续重试。跨过午夜后，既存pending原生发布任务仍由同一路径恢复；已有原生名单的影子捕获继续消费其冻结D/T和分数来源。

首个正式预测必须在2026-11-03 **09:30之前**完成并保存。09:29开始但09:30之后才完成也不能算有效预测。没有原生发布来源时保持WAITING_NATIVE_PUBLICATION；不能手工另选Top50代替。若完整决策日离线、没有原生发布且没有已登记pending任务，不承诺凭空恢复当时名单；过目标开盘仍无有效预测时，V2按不可恢复捕获缺口处理。

H收盘只是最早成熟时点。之后仍需原数据能给出固定估值结果；不能承诺15:00立即产生完整收益。行情、复权、涨跌停或停牌信息不能支持原合同结果时保留UNKNOWN，由既有调度在原期限内重试。当前最新行情只用于显示，不能替代T开盘至T+4收盘的固定标签。

## 既有工具与执行顺序

1. 只读检查`GET /api/v1/advisory/forward/status`的running、thread_alive、last_error及lstm_spo_shadow结果。只读API不会启动捕获。后端启动/停止/重启仍由用户负责。
2. 只读检查`GET /api/v1/advisory/workbench/programs/advp_ed918504a02f4b15892f0dba65da139b/readiness?data_date=<D>`。以该精确包的实际依赖为范围；不因其他包资产缺失或其他指数缺价阻断它。
3. 复用下列health命令，仅核对官方日历和原预测/MISS，不读收益。当前calendar-20261101.json覆盖到2026-12-31；不能用它宣称150D日历已齐。

```powershell
python scripts/advisory_lstm_top10_business_review.py health --protocol X:/AIstock_temp/ADVISORY/lstm-business-review-20261101/study/preregistered/protocol.json --calendar X:/AIstock_temp/ADVISORY/lstm-business-review-20261101/calendar-20261101.json
```

4. 自然捕获后，通过`GET /api/v1/advisory/programs/advp_ed918504a02f4b15892f0dba65da139b/reranking-shadow?target_trade_date=<T>`核对原50行、双Top10、prediction SHA、observed_at和源引用。数据版本在计算期间改变须等待/重试；预测保存后不能因当前revision变化重做旧名单。
5. 自然成熟后，对账已保存的两臂净收益、差值、十槽分母、现金/未知槽、逐股贡献和四项损益分解。只报告同组完整配对描述，并保留全部未完整批次分母。当前任务不主动读取新收益标签或调用finance补做评价。
6. 后续官方日历真实到位后，在已加载既有数据库环境配置的操作进程中，复用既有`calendar --output <V2证据根下新的唯一文件名>`只读导出，不覆盖旧快照。该CLI不会替操作进程自动加载`.env`；配置位置为canonical根的既有`.env`，只记录位置，不输出环境值。核对新快照与原已冻结范围的差异及原D/T身份；修订显式报告，不静默改预定日期，不猜2027交易日。

health的PLANNED、CAPTURE_WINDOW_OPEN、WAITING_CALENDAR分别保留，不把未到期或未来日历不齐当成已漏捕获。到期且无合法预测才形成IRREVERSIBLE_CAPTURE_GAP。若确认不可恢复缺口，按原`finalize --protocol <原协议> --calendar <核验后的快照>`落一次NOT_EVALUABLE_CAPTURE_GAP，0收益读取；不另起研究目录或用第151D替换。可修复的UNKNOWN金融评价仍保留原宽限。

## 持续评审与责任边界

唯一正式协议仍为V2，producer `554e72ed3f71bc1e373c46ff3f9386f0d36a5b65`、protocol SHA `0e9c51566efaebb59df2d5f9d0482b78f6a9a29af405921e47d772baa738e9d3`，从11月2日起首150个预定D。最后D不晚于2027-07-31，最后H后5个会话为既定判断时点，绝对最晚2027-08-31结束。没有新增中途显著性检验、扩样、候选或模型预算。

正式判断分开报告μ>0统计支持与点估计是否达到100bps。两者满足只意味着值得业务评审，不自动启用，不证明真均值超过100bps或真实成交可获相同收益。可评价但目标未满足时保留原结果；不可评价时报告缺失原因，不推断无alpha。中途描述性结果不用于单日调参或重开旧负方案。

本窗口负责精确LSTM消费、捕获身份、固定评价和研究去留证据；数据管理负责源同步、就绪台账及revision。既有其他包资产问题及全局历史告警单列，不纳入本次源码范围。研报、现金流、买卖最优价和其他包扩展继续暂停；正式业务接入/生产替换另行确认。

## 本轮回归与验收矩阵

新增`backend/tests/advisory_model_first/test_lstm_forward_acceptance.py`，复用现有隔离trial fixture；所有模型、时钟和数据库对象均为合成，不产生真实前向证据。11项连续场景覆盖晚到/跨午夜/09:29就绪及读前/计算后等待、09:30和10:00不可补捕获、revision变化重试、其他包失败隔离和真实OS文件锁竞争后恢复。测试使用进程内新服务对象模拟恢复，不控制任何用户进程。

| 规划条款 | 实现或证据 | 本轮状态 |
| --- | --- | --- |
| 数据就绪、输入范围与revision | 既有inputs/verify_capture；真实live-input-readback；新增连续场景 | PASS，限先行批次的当前输入 |
| 晚到/午夜/幂等与失败隔离 | 既有ForwardService/ShadowService；新增11项回归 | PASS，合成场景通过 |
| 首个正式批次真实捕获 | 原调度、health及只读业务API | NOT_DUE，11月2日D/11月3日T |
| 两个首批真实成熟验收 | 原finance/paired_result、已保存评价与业务页 | NOT_DUE，11月6日及11月9日H |
| 持续完整性与单次价值判断 | 原V2协议/health/finalize，固定150D与官方日历 | 工具已存在；未来真实结果PENDING |

本轮工程与当前可执行验收可独立完成；不把未来任务标PASS，不让整个工程交付挂起等待数月样本。未修改冻结十文件源码闭包，无后端重启、DDL/DML、依赖、客户端、QE或生产激活要求。
