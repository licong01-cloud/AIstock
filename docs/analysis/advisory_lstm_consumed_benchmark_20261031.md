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

原 publication、package、binding、Selection/分数资产及原始冻结输入证明均保留。模型文件、原始Top50、双Top10排序、5TD、成本、持仓和漏采时间规则未改变。

## 用户批准追加：冻结原分数与当前特征分别绑定

用户在本轮异步确认“同意追加合同修复（建议）”：保留原名单、原分数资产和旧绑定，验证其不可变来源；对实际读取的20TD特征另绑定当前revision并在计算后复核，替代对未再次读取的原模型全历史数据重复校验。

实现合同 `FROZEN_NATIVE_SCORE_AND_CURRENT_FEATURES_V1`：

- 原 publication 的ID/D/T/时间/状态、当前精确Program/active binding、package manifest及准入状态、当前PIT身份必须匹配，不能以旧包或另一个股票池替代。
- 只从原 Selection 配置解析唯一分数资产。重新校验分数内容hash、原生实际消费分数逐项一致、原始Top50、原绑定hash及原读取快照中逐分区的冻结证明。原筹码revision=2等原记录保持原值，不换成当前版本。
- 此阶段消费的是冻结分数资产，不再读取训练窗口或原预测历史事实表。历史分区随后revision=3不重新定义原分数来源；不冒称原事实已经按新版本重新计算。
- 特征读取前和模型计算后，分别重新解析原版本/资产/配置并比较完整来源证明，同时复核当前实际20TD特征证明。原资产、原绑定、PIT/准入或实际特征发生变化仍阻断。
- 原绑定与本轮特征分别落入预测收据，公开标记CURRENT_DATABASE_NON_VINTAGE，不宣称历史PIT或实际成交。

## 输入回执与独立验证

用户指定目录 `X:/AIstock_temp/DATA_PREPARATION/monthly-october-release-20261030/`：

- `advisory-postrestart-readback-20261031-r1.json`，SHA256 `2dbd7bc1ba1586a5db3b232a9a02975525d5f7f8edce5d7ea75fcc448dac1333`：四包 READY；额外20TD原分区58 VALID/1 INCOMPLETE/1 UNVERIFIED；不证明整个指数分区通过。
- `advisory-benchmark-readback-20261031.json`，SHA256 `9661645219cb0fbd89ca4a4199e69c7aecfe232c1b83ef3e77a1d649837a7f50`：实际20日沪深300有效，负例 PASS，全局台账未修改。

本轮新进程从独立任务树实际读取 D=10月30日、2026-09-28～2026-10-30 的20TD，使用修改后的原生 `extra_input_capture` 得到 PASS；沪深30020/20，10月19日和28日的全局指数负面验证状态仍保留。收据 `tmp/issue_workflow/BUG-2159/fresh-readonly-20261031.json`。未拟合、未读收益、未写预测或数据库。

## 追加合同前的原生阻断与后续读回

完整 `LstmSpoShadowSource.inputs` 仍在原名单版本复核时失败：

- 原名单 `advlv_6c91583f3d67dad5be4aa9e11872fe26`，Selection `sel_63ea92da191e4f1d933c0cb6598161d5`，D=10月30日/T=11月2日。
- 原冻结 binding 的 `cyq_perf/2026-05-20` 为 data_revision=2；当前已为3，validation_id也变更。原证明将000663.SZ缺失作为NaN保留；全窗口当前 READY 不能证明原绑定未变化。
- 指数范围第一版没有改写原binding或放行旧版本，完整读取明确BLOCKED；该负面收据保留。随后用户明确批准上述独立合同，本轮按冻结资产资格与当前实际特征分别校验，替代旧全历史live检查，绝不伪称原binding未变化。

追加合同的新进程只读验收 `tmp/issue_workflow/BUG-2159/contract-readonly-20261031.json` 已PASS：原分数4918行、原生消费40行逐项一致，原Top50保留；50行八字段全部可算，计算后来源及20TD版本复核通过。原名单、旧绑定和台账未修改，未写预测或读取收益。

源码合入后执行一次原生影子捕获并做独立读回，状态另记最终交付收据；尚未落盘不能提前关闭BUG。目标仍须在11月2日09:30前完成，成功后最早11月6日收盘后评价5TD；不重复发布原名单。

## 多轮审核与验收矩阵

| 用户要求 | 实现与证据 | 结论 |
|---|---|---|
| 实际指数/close/20TD/当前revision | 原生消费证明、独立20/20读回 | PASS |
| 其他指数缺价不误阻断；全局负面保留 | 全局INCOMPLETE负例、真实10月19/28日读回 | PASS |
| 缺行/NaN/重复/revision变化阻断 | 原25项定向回归及追加冻结资产/准入/PIT回归 | PASS，最终矩阵见交付收据 |
| 模型/Top50→Top10/5TD保持 | diff限定输入源及测试；原既有模型/时钟/人口回归通过 | PASS |
| 原历史漂移与当前消费分开绑定 | 保留原冻结证明；真实4918/40/50输入及当前版本复核PASS | PASS |
| 原生首份影子捕获 | 合入后原生落盘及API独立读回 | 待最终交付收据，不提前报成功 |

第一轮审查范围和数值拒绝路径；第二轮审查独立快照、计算后revision变化、原绑定及前向时钟；追加合同复核冻结来源、当前PIT/准入及原资产修改的拒绝路径，最终逐项核对上述矩阵。四项 DESIGN-COMPLIANCE-001：不把消费范围PASS冒充捕获完成；错误和历史负面可见；用户已批准资格合同调整，原模型和名单语义保持；不新增人工审批或等待门槛。

生产DDL、数据库写入、依赖安装、前端激活均为noop。源码/CI合入与后台加载分别验收，后端重启仍由用户执行。工作树和证据在完整验收前保留。
