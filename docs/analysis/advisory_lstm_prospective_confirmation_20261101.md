# LSTM-CONFIRM-1：前向确认预算与真实预检

2026-11-01。**本轮长任务的设计、文件型工具、真实冻结和预检已完成；没有新增训练或证明新收益。** 原P20继续自然观察，本工具提供有期限、可复核的后续判断方法，不修改荐股发布条件。

设计见[LSTM-CONFIRM-1 F1](../architecture/advisory_lstm_prospective_confirmation_v1_f1_design_20261101.md)。精确LSTM包、P19第3折、原Top50→双Top10、5TD、费用和UNKNOWN/CASH固定；生产Top20不变。LSTM-VALUE-1已经完成，不重跑它或P18/P19。累计实际fit仍248＋1旧index。

## 已完成什么

`scripts/advisory_lstm_prospective_confirmation.py`提供freeze/calendar/inspect/finalize。冻结单一假设与固定150个预定D，从2026-11-02开始；10月30日先行预测保持但不入本次确认。最后D不晚于2027-07-31，最后H后5会话修复宽限结束才做唯一正式统计判断，最晚2027-08-31结束。漏捕获、UNKNOWN和缺结果保留，全人口不齐则NOT_EVALUABLE，不换日或补零。

正式工具复用原模型预测器和双臂固定槽估值，对原包/程序/绑定、开盘前时钟、完整50行、固定6会话和评价父hash逐项核对。评价按截止前首个完整版本，修复晚于截止不改变本次终态。协议固定唯一study目录；终态create-only，重跑复用，不能用新目录另造正式决定。收益只消费P20已保存评价，无raw收益源，无自动激活或新调度。

固定主统计是lag10 Bartlett/Newey-West HAC的单侧95%下界，lag4/20仅敏感性；合成数据的独立矩阵与实现一致。正支持仍要结合两臂绝对净估值、下尾/最差批次、干预和执行能力评审；工具附这些描述性数据，不能称为账户NAV或真实成交。零均值界限不是已经确定的业务最低收益。统计支持不会自动写入运行时经济确认字段或替换推荐。

## 预算的实际含义

仅复用旧report的102共同D、SE=27.291809958209306bps，长期波动代理275.633769bps；不读取旧收益面板或新标签。按单侧α5%/power80%的条件近似：

| 增量情景（bps/5TD批次） | 旧噪声代理下所需完整D | 噪声升至1.5倍时 |
|---|---:|---:|
| 25 | 752 | 1,691 |
| 50 | 188 | 423 |
| 57.277530（旧开发均值，仅情景） | 144 | 323 |
| 100 | 47 | 106 |

150D预算可识别增量的80%把握度近似MDE分别为55.959119 / 83.938678 / 111.918238bps（噪声1 / 1.5 / 2倍）。规划公式与限制见[NIST](https://itl.nist.gov/div898/handbook/prc/section2/prc222.htm)。旧噪声混合三个不同折模型、开发复用、重叠5TD及完整组选择，不能保证固定第3折的未来功效。HAC也依赖弱平稳和混合条件，不保证非平稳市场的精确误报率，参见[官方HAC前提](https://www.statsmodels.org/stable/generated/statsmodels.stats.sandwich_covariance.cov_hac.html)。

**不能承诺几天、几十天就能证实约50bps的小增量。** 150D约数月到七个月，具体日期必须按真实日历；噪声放大或更小增量不在本预算的可靠识别范围。固定预算完成仍可能INCONCLUSIVE，不为显著性延长。全150D完整要求是本研究可识别性的约束，任一不可恢复漏日将使正式研究不可评价；它既不证明没有alpha，也不阻断原生推荐/P20日常运行。

## 真实运行和边界

固定producer `bcc96c9609b11dc228e9e7afe8a8561302ea9138`；protocol SHA `9b78a785c44158fb689089514756d0a590a28db5d34d80a531b53cd15cb094e2`。不可变工件根`X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101`，协议及预算在`study/preregistered/`。

实际只读日历导出60个自然日（2026-11-02～2026-12-31），SHA `d1fc6b680b93548b875b7db22ffb7ad88d29d1084cd553f4d5f001b928b4ef65`；未来2027日历尚未在源表中提供，保留WAITING_CALENDAR，不要求数据窗口现在补猜测日历，也不把它当当前荐股故障。日历通过既有REPEATABLE READ/readonly连接读取，不写数据库。

真实inspect：本次确认预测文件0，先行D=10月30日明确排除，0评价文件读取。真实finalize预检：WAITING_CALENDAR、0评价文件读取；未生成提前的evaluated终态。新进程第二路径拦截所有evaluations文件打开以及fit/live finance/调度入口：12个预算情景经SciPy正态分位另算全部一致，HAC合成矩阵一致；真实先行50行预测逐项重算通过，模型/release/旧report/先行预测物理hash全部保持。新进程freeze再次调用PROTOCOL_REUSED。

初次工作树日历调用缺既有数据库环境变量，明确失败；随后仅在进程中从既有`F:/Dev/AIstock/.env`装载配置，重试成功，没有复制或输出凭据。该失败不算数据缺失、fit或收益研究尝试。

CLI在已配置环境中使用：

```powershell
python scripts/advisory_lstm_prospective_confirmation.py inspect --protocol X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/study/preregistered/protocol.json
python scripts/advisory_lstm_prospective_confirmation.py calendar --output X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/calendar-NEWDATE.json
python scripts/advisory_lstm_prospective_confirmation.py finalize --protocol X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/study/preregistered/protocol.json --calendar X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/calendar-NEWDATE.json --output X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/study
```

上面NEWDATE是显式新快照名，不能覆写旧快照。后两步只在确需检查时调用，没有新增后台任务。原P20可以按原设计展示每日描述性结果；整个系统并未盲化，正式推断仍只用固定一次判断，禁止因中间结果调模型或预算。

## 审核、验收和后续顺序

R1数值/时钟自审发现JSON列顺序变化导致合法预测比对失败，修正为按列名对齐，行次序和数值仍严格核对；失败节点重跑通过，真实50行也通过。R2研究自审区分旧噪声情景与未来固定模型、总体未齐与无alpha、统计支持与业务上线，补足P20非全系统盲化及有限预算。R3交付自审绑定唯一study_root、协议原路径、截止版本和不可变终态，禁止更换输出目录形成多份正式结论。这些是本窗口自审，不是独立外审或新经济证据。

22项直接测试通过，覆盖完整合成成熟路径及UNKNOWN/缺预测/时钟/身份/修改结果/日历错误/唯一目录/零拟合。合成研究只是合同测试，不能计入真实前向结果。Ruff/compile、ownership和精确CI路由通过，较宽模块回归交现有CI/Nightly；完整F1、蓝图F2、L0及PR/CI/合入状态由validation_receipt.json/delivery_receipt.json保存。没有未批准的简化、占位或自动成功路径。

下一顺序：保持冻结模型和原观察；新D自然保存/成熟评价；待真实日历可用时新建快照；在固定时点对全部150D做一次正式判断。正支持后审执行/效应/风险，未支持则停止该确认，不扩窗救结果。只有具体新事实支持不同机制时另写单一候选及预算，当前0新候选、0fit，不为了等样本填充无关研究。本轮工程任务结束，不在窗口挂起等数月数据。

无需后端重启，无DB DDL/DML、依赖/客户端/数据集变更或QE；未改变生产模型、原生推荐或P20调度。工作树/分支保留，不推导清理授权。经济确认和生产排序替换仍为false。
