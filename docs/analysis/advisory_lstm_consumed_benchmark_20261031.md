# BUG-2159：LSTM 实际消费的沪深300收盘价验证

日期：2026-10-31。用户授权修复实际消费范围；四包原生荐股已发布且当前 READY，禁止重复发布。对应 Issue #6960。此文记录 BUG 修复及剩余验收条件，不新增研究或模型。

## 问题与修复

LSTM 的六个行情特征只需要 D 截止的二十个权威交易日和 `000300.SH/close`。原 `extra_input_capture` 和计算后 `verify_capture` 要求整个 `index_daily` 分区 VALID，导致其他指数缺少收盘价时，已完整的沪深300也被阻断。

本次在相同 REPEATABLE READ READ ONLY 快照中验证：

1. 日期必须等于权威交易日历中 D 截止的最后20TD，不接受任意二十个日期。
2. `kline_daily_raw/adj_factor` 继续使用原完整性校验及原版本绑定。
3. `index_daily` 必须有20份明确的当前 revision、现行 scope/rule 元数据；以 `000300.SH` 和实际20TD作精确查询，要求每个日期恰好一行、close为有限正数。NULL、NaN、无穷、非正值、重复、缺行、外来键均失败。
4. 独立消费证明保存指数、字段、完整日期、当前分区版本、全局验证状态和实际收盘价内容 hash；`whole_index_partition_certified=false`。其他指数的负面状态保留，代码没有数据或台账写入。
5. 模型计算后在新的只读快照中重读相同范围，重新验证并比较全部证明；内容、revision或验证身份发生变化均拒绝落盘，下一次合法调度可重试。

原 publication、package、binding、Selection/分数资产及原始输入绑定检查均保留。模型文件、原始Top50、双Top10排序、5TD、成本、持仓和漏采时间规则未改变。

## 输入回执与独立验证

用户指定目录 `X:/AIstock_temp/DATA_PREPARATION/monthly-october-release-20261030/`：

- `advisory-postrestart-readback-20261031-r1.json`，SHA256 `2dbd7bc1ba1586a5db3b232a9a02975525d5f7f8edce5d7ea75fcc448dac1333`：四包 READY；额外20TD原分区58 VALID/1 INCOMPLETE/1 UNVERIFIED；不证明整个指数分区通过。
- `advisory-benchmark-readback-20261031.json`，SHA256 `9661645219cb0fbd89ca4a4199e69c7aecfe232c1b83ef3e77a1d649837a7f50`：实际20日沪深300有效，负例 PASS，全局台账未修改。

本轮新进程从独立任务树实际读取 D=10月30日、2026-09-28～2026-10-30 的20TD，使用修改后的原生 `extra_input_capture` 得到 PASS；沪深30020/20，10月19日和28日的全局指数负面验证状态仍保留。收据 `tmp/issue_workflow/BUG-2159/fresh-readonly-20261031.json`。未拟合、未读收益、未写预测或数据库。

## 原生捕获的独立剩余阻断

完整 `LstmSpoShadowSource.inputs` 仍在原名单版本复核时失败：

- 原名单 `advlv_6c91583f3d67dad5be4aa9e11872fe26`，Selection `sel_63ea92da191e4f1d933c0cb6598161d5`，D=10月30日/T=11月2日。
- 原冻结 binding 的 `cyq_perf/2026-05-20` 为 data_revision=2；当前已为3，validation_id也变更。原证明将000663.SZ缺失作为NaN保留；全窗口当前 READY 不能证明原绑定未变化。
- 本轮没有改写原 binding、替换其版本或绕过 `verify_current`。指数消费修复 PASS 与完整捕获 BLOCKED 分别报告；不能据此关闭 BUG-2159 或 BUG-2111，也不能把旧预览/漏采记作前向成功。

后续必须明确解决冻结原生分数资产与已修订历史依赖之间的读取合同，或在下一新原生发布的合法窗口完成验证；不得重复发布已发布名单或伪造旧版本。当前目标仍须在11月2日09:30前完成真实捕获；若成功，最早11月6日收盘后才评价5TD。本轮不提前读取该收益。

## 多轮审核与验收矩阵

| 用户要求 | 实现与证据 | 结论 |
|---|---|---|
| 实际指数/close/20TD/当前revision | 原生消费证明、独立20/20读回 | PASS |
| 其他指数缺价不误阻断；全局负面保留 | 全局INCOMPLETE负例、真实10月19/28日读回 | PASS |
| 缺行/NaN/重复/revision变化阻断 | 新增25项定向回归；最终两文件53项全部通过 | PASS |
| 模型/Top50→Top10/5TD保持 | diff限定输入源及测试；原既有模型/时钟/人口回归通过 | PASS |
| 原生首份影子捕获 | 完整输入读取被历史筹码原binding漂移拒绝 | BLOCKED，未完成 |

第一轮审查范围和数值拒绝路径；第二轮审查独立快照、计算后revision变化、原绑定及前向时钟；最终逐项核对上述矩阵。四项 DESIGN-COMPLIANCE-001：不把消费范围PASS冒充捕获完成；错误和历史负面可见；不改原模型及业务语义；不新增人工审批或等待门槛。

生产DDL、数据库写入、依赖安装、前端激活均为noop。源码/CI合入与后台加载分别验收，后端重启仍由用户执行。工作树和证据在完整验收前保留。
