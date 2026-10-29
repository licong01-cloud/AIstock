# LocalSIM 冻结研究的仅报告版本化恢复

2026-10-29，BUG-2134 / Issue #6871。补齐 BUG-2125 的正式消费合同，不重新运行账户，不将旧 request 的源码 pin 改为当前实现。

## 最小设计和写入边界

在现有离线 `dual_mode_research` CLI 增加 `resummarize`。调用必须显式绑定原 request/results 的绝对路径和完整 SHA、当前报告 producer commit 与独立输出根。原 `run` 继续严格拒绝源码身份变化。

先使用原生来源检查完成整批候选、价格计划、费用、PIT、策略包及日期人口核验，再读取原账户结果。逐日验证完整文件 SHA、前日引用、原生 generation/receipt、共同期初与 T+1 开盘账户，以及既有 `validate_day_ledger` 的成交、现金、持仓和父订单费用读回。禁止执行选股、择时、市场撮合或账户日计算。

仅调用唯一 `summarize_accounts` 重建比较；账户摘要、净值、费用、所有账户日和原 producer/release 身份原样保留。新报告在独立目录生成不可变 request/results/result_receipt 和恢复 receipt，明确登记新的报告 producer、完整实现指纹及来源结果。`report_only_revision` 区分账户计算身份与报告修订身份，不冒充旧 producer 的 exact retry。

原研究目录、status、断点、源码、日结果、账本和 results 全部只读。原 FAILED/EXACT_RETRY 保留。新修订自身 exact retry 返回 `ALREADY_MATERIALIZED`、重算账户日为零，旧文件任何漂移或新修订冲突均拒绝。发布前再次核对已消费原产物的 SHA，不覆盖旧结果，不对差异做容差归零。

结果沿用现有 `localsim_dual_mode_results_v1` 账户合同，并保留原 request/source_inspection 的绑定；新 receipt 可供既有共享连续分析消费。分析对原任务复现状态的判断保持其正式合同，本窗口不修改比较器或把原任务状态伪造成成功。

允许修改：本离线入口、现有直接测试、本报告和 BUG JSON。无数据库、行情网络、数据集、服务或生产账户操作；无需用户后端重启。

## 验收索引

| 条款 | 验证 | 状态 |
|---|---|---|
| 来源先于结果、冻结输入不变 | 输入失败时禁止打开结果；真实历史完整来源检查另列 | 定向通过 |
| 全人口与原生账本/费用核对 | 完整账户日及 generation 校验；缺失/篡改负向测试 | 定向通过 |
| 不运行新账户或改写旧身份 | 新源码 `run` 仍拒绝；仅报告路径无账户计算；旧文件 SHA 不变 | 定向通过 |
| 唯一汇总和不可变 exact retry | 固定排序汇总；同修订二次调用字节和 SHA 一致 | 定向通过 |
| 181日、13账户真实消费 | 2353 账户日、共同终点及原结果完整 SHA；无账户日重算 | 通过 |
| 现有分析兼容 | 共享分析完成来源 preflight 后消费新 result_receipt；经济结论单列 | 消费与重试通过；原producer复现标记仍未验证 |

直接测试、多轮自审、L0、CI、源码合入、真实历史恢复及经济有效性分别记录。不得以技术修复宣称收益提升。

## 直接验证与审核

新增入口前 fix-point 因不存在正式恢复能力失败。首轮正向测试已走真实原生账户/撮合/费用夹具；源码升级后原 `run` 继续拒绝，新增 operational 错误收据属于该拒绝调用的既有行为，测试在其后冻结原目录，确认仅报告路径不添加或修改旧文件。

两项篡改场景首次测试预期异常类型错误：共享 `checked_file` 实际已通过 `IntradayError/RESEARCH_SOURCE_IDENTITY_MISMATCH` 拒绝，未发布结果；仅纠正直接测试的精确异常类型，两节点复跑通过。费用差额至极小 Decimal 末位的新报告冲突继续严格拒绝，不截断或容差归零。

完整相关单文件 `report-related.xml`：23 项通过，覆盖原研究、跨进程确定性、旧源码拒绝、无账户日计算、T+1、完整前缀、来源先于结果、不可变新修订及新修订重试。L0 首轮 0 findings/0 blocking；最终源检查和 CI 分开记录。

审核一核对仅报告边界和原账户 producer；审核二核对账户人口、共同14:57、完整期初/开盘/日账本/费用及结果引用；审核三核对原目录保护、原失败留存、新产物幂等和共享分析兼容。均为本窗口自审。DESIGN-COMPLIANCE-001 四项：无未批准子集；无吞错或假成功；无交易语义变化；无新增审批或数据门禁。

## WSL 全量冻结历史验收

真实研究保持2026-01-05至09-30、181日、13账户、2353账户日，账户日重算为0。首次正式报告修订耗时656.63秒，峰值RSS 1312828 KiB；重复调用605.05秒，返回 `ALREADY_MATERIALIZED`、同一结果SHA。重复核验重新读取完整来源与原账本，耗时不是账户回放性能。

验证脚本禁止 `socket.connect`；数据集、模型、原研究和生产数据库均未改动。前后核对4712个原JSON/状态文件完整SHA全部相同，原 `FAILED/EXACT_RETRY` 保留；新结果仅在独立目录发布。首次WSL命令因Windows worktree的git指针无法解析，在消费结果前退出；改由Windows核验并显式传入producer commit，未改变原研究身份。

证据根目录：`X:/AIstock_temp/localsim-reliability-followup-20261029`。

| 产物 | 完整文件SHA256 |
|---|---|
| report-real-run-r1.json | 7ee146021d1376706749be9073b7efcd5dc03ca008c80af3c4fc3ddc76cbde61 |
| report-real-retry-r1.json | bd12cd4fce7b910357a4dac7a1b304e39ba8eff27e39d131f6a0e44fe81a8a76 |
| report-origin-protection-r1.json | 12c7a2472870e3c378e50e7727857ec354ecee577381853c6f8bfe022416556d |
| 新 results.json | 1b0a0d2c6bac4beedbaf3107ef3f96303730da9c9b4b31e840e20d4cd359d4c0 |
| 新 result_receipt.json | ae15003f342b502ef5d460378b16f4c25cbbedc1f2d29b756c95d201450ab3f2 |
| 新 recovery_receipt.json | bce0d78519a72e47a1351d6f66ed2c3daa22e4e007dab07cb07c32b271fa9ec9 |

新结果绝对路径：`X:/AIstock_temp/localsim-reliability-followup-20261029/report-revisions/e9c1c21038fdc516721ae1c6bb7cf7bca6d2ca00c266071cad072bddd043f227/results.json`。报告producer source为 `e20fb725cd08e064dd2fd6cfadc6bd0679249c26`，原账户producer仍是 `ff41587e8f665bf48b4bdb6d0700e4a8225688e8`；后续只补交付文档/BUG记录，不回写已生成身份。

## 共享连续分析与经济边界

未修改共享分析器。既有 `continuous_results.analyze` 先完成来源检查，再消费新result_receipt，生成13账户24比较，分析exact retry同样返回 `ALREADY_MATERIALIZED`。分析文件 `report-analysis/intraday_v1/continuous_analyses/3875c834772ef1e9874f061271d0dd1dad5bfa3423c73df614225334a1c8a69b.json`，SHA256 `d6671714eb629b37b42e92f63b8614bc199d4428a7effcb77c7355ebbac8f06b`；消费回执 `revision-analysis-consumption-r1.json` SHA256 `0c3d5535fc99d7f2d54587e61e9a6e16d1cf416df00473712f21fe1f93c3948c`。

共享分析仍标记 `PROVISIONAL / REPRODUCIBILITY_UNVERIFIED / PRODUCER_RETRY_EVIDENCE_ABSENT`：原status绑定旧结果，不能作为新报告producer状态传入。新修订重复核验已通过，不等于旧账户producer exact retry已通过。如共享展示需识别“仅报告修订可复现”，由择时owner增加独立证据合同，不得将旧任务改为成功。

此次只恢复汇总及证据消费，不改变历史账户净值、费用、成交或研究参数，也不构成经济有效性的新证据。部分180/300秒配置没有成交、收益与费用均为零，应解释为未获得敞口，不能作为优于TWAP的依据。
