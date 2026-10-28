# LocalSIM共享批量Reader消费验收与BUG-2112

日期：2026-10-28。状态：共享Reader真实消费、BUG-2112修复后三日完整账户一致性、恢复和exact retry通过；修复尚未建立PR或合入，运行目录合同阻断保持。本文不代表生产回放或经济有效性验收。

## 输入、边界和源码

共享接口为PR #6791（`acc60e14c69e03e3926d2af8455167e64144ddfd`）的唯一CandidateReader；最终CI成功。四份共享市场对象/来源pins证据已按交付报告的完整SHA独立读回。Reader LF源码SHA为`d2023f4e1f551ccb8be5c1557a5cd000952384ea65ab1ba7cfa08f0e8c8a5717`。

首轮LocalSIM验证工作树冻结在`68f76d98ec63d390706ac7d967e03d6c18557148`，证据根为`X:/AIstock_temp/localsim-batch-reader-acceptance-20261028/`。独立新冻结研究采用2026-01-05/06/07、FULL_13、每账户现金1000万元，原生TWAP及三规则乘四响应延迟共13个账户。两种模式均使用optimized及2个账户进程，仅batch_days从1改为3；同一新request的字节副本放在独立输出目录，不共享账户或产物。

候选完整SHA为`321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`，来源continuous request完整SHA为`a9ab2c0388702fbe746061b466083706d189dd2f7d4f29a6c4fba6c7a0a14668`。费用文件SHA为`d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041`，执行合同SHA为`e89ae9b5103c0a5cab3fbb9b8bbb86273b8a24cadd3f8e18309b4b0f562d8140`。共同终点14:57、PIT、冻结计划及政策均保持。未训练QE、未查写数据库、未改数据集、未抓行情或控制用户服务；活动181日及旧288产物只读保护。

## 首轮真实账户结果

两种模式各完成39个账户日。来源整批检查均先于账户结果，`outcomes_read_stage=AFTER_COMPLETE_SOURCE_BATCH_INSPECTION`。逐笔449次成交及资金、费用、持仓、完整执行状态和收益汇总一致；全体账户包含189笔订单、1895个父计划和9282个分钟净值观察。保留完整账户receipt实体及其文件SHA。

严格比较发现12处maximum_queue差异，涉及6个账户日；不得将该统计差异静默排除后声明完整状态一致。原始反例见`pair-difference-inventory.json`。对相同已验证开盘账户上下文进行身份对齐后，剩余差异仅maximum_queue，原生经济facts及执行states一致，见`opening-context-difference-diagnosis.json`，完整SHA为`711387217c621ef331a88cc60f93de922f63d20201120e4e8f8f41e15cdc4475`。

独立功能回归在两边均使用逐日模式、只更换输出目录时仍复现第二日run_id变化，证明不是批量Reader才能触发的问题。正式回归修复前失败，见`bug2112-regression-before.xml`；外置复现见`cross-root-repro-before.xml`。这些小fixture只证明程序合同，不冒充真实历史经济证据。

## 性能口径

WSL使用现成环境、OMP/MKL/OpenBLAS线程1，原181日任务同时运行；宿主页缓存不可控，每模式仅一次，不宣称冷磁盘或统计显著。

| 同为2worker/optimized | 逐日 | 3日批量 |
| --- | ---: | ---: |
| 完整CLI秒 | 1098.0113 | 1083.8424 |
| 来源核验秒 | 325.7779 | 287.7673 |
| 行情准备秒 | 68.0688 | 91.0420 |
| 账户任务wall秒之和，不能当端到端秒 | 1326.4965 | 1330.0710 |
| 内部扣来源核验后秒 | 761.0427 | 784.5217 |
| 进程树峰值RSS KiB | 1646032 | 1657600 |

总耗时仅减少约1.3%，同时来源核验快约38秒、行情准备慢约23秒。此样本不支持把1.3%归因为批量读取带来的稳定提速；扣来源阶段反而约慢3.1%。共享owner报告的读取链路约1.28倍不能直接外推到账户全链。本轮不自动扩大窗口或调整人口以寻找正结果，也不修改旧默认参数。资源观察器早期遇到drvfs状态文件替换的瞬时读取缺口并保留标记；主驱动的RSS采样持续，FD观察仅按其实际覆盖报告。

## BUG-2112修复和前缀保护

旧实现将previous FileRef的物理路径及完整日报SHA纳入账户日input_identity；日报还含处理时钟。该身份继续生成plan/order/queue身份，并影响同时刻计划遍历及排队峰值。因此，相同冻结业务输入在独立输出目录或独立重算时不能完整复现。

修复在LocalSIM的`_account_day_identity`中仅绑定冻结request、账户、日期及实际消费的前日现金、持仓和参考价。两处任务/提交身份计算复用该函数。前日物理FileRef、完整SHA、原生journal、开盘核对和不可变发布检查全部保留。改变业务输入会改变身份；改变纯审计字段不会。源码身份保护继续拒绝新源码续接旧request，不迁移活动181日。

最小反例修复后通过；后续10项相关定向检查通过，覆盖优化/旧路径、跨目录两日完整业务facts、业务输入敏感性、篡改已提交日报拒绝、并行失败前缀、恢复/exact retry、批量入口、部分成交/取消、原生估值及源码升级拒绝。证据为`bug2112-regression-first-fix.xml`与`bug2112-focused-matrix.xml`；两份L0扫描均为0项发现。更广回归仍待最终HEAD CI，不将直接测试替代CI。

## 修复后完整三日验收

源码冻结在`627fd4fb2638d6e8c74c94a079e980ee3a53f452`，独立验证工作树为`F:/Dev/AIstock_worktrees/localsim-batch-reader-fixed-acceptance-20261028`。未热更修复分支或旧181日工作树。证据根为`X:/AIstock_temp/localsim-batch-reader-fixed-acceptance-20261028/`；新request完整SHA为`9642c82caadbd681edfb5217f1be2d75570afcb4601b24971788394d98a0268b`。

两边均完整覆盖13账户、2026-01-05/06/07及39个账户日，各有449笔成交/资金流水、189笔订单、454个OMS事件、1895个父计划和9282次分钟净值观察。184个父计划FILLED、5个PARTIAL、1706个NO_FILL、0个UNAVAILABLE；未删未成交或部分成交计划。原生账户第二、三日均有50只期初持仓和实际卖出，人工账户也有持仓结转和卖出，覆盖真实多日换仓。

严格读回比较完整账户汇总、逐日持仓现金、逐笔成交/订单/事件、费用、NAV/MDD、原因、人工队列和执行状态。run/plan/order身份不做对齐或重写，核心input/previous-state SHA、idempotency和maximum_queue不排除。只按既有设计分离经自身哈希验证的处理时钟和原生无成交取消/拒绝随机事件ID；物理前缀和receipt实体先验完整SHA，再比较对应完整业务对象。原始文件仍保留全部审计字段。9项反证检查确认成交ID、数量、价格、交易时刻、核心哈希、队列指标、幂等键及事件重复不能被归一化吞掉。

恢复以正式`--stop-after-account-days 14`停止，保留14份日报和14份原生generation，接续仅计算剩余25日；28个前缀文件SHA均不变。随后exact retry返回`ALREADY_MATERIALIZED`、`computed_account_days=0`，结果SHA保持`fa3451a272f89549c1df0c9c1910b5035ee88698296f516212c384d6c1be420e`，整个研究目录全部JSON逐文件SHA不变。读取账户结果阶段始终为`AFTER_COMPLETE_SOURCE_BATCH_INSPECTION`。

| 收据 | 完整文件SHA256 |
| --- | --- |
| execution-receipt.json | 5fd18189b6a55b85a677cce0e74fe0b77fb1ff76054fc5b40d42ad27c0d9c1c8 |
| strict-equivalence-receipt.json | d4a9b98eb1b08871c19ad8d33549cdece8b8e08597c7c00cc267556a5409cfd1 |
| final-consumer-acceptance.json | 7296b7e4b672ec1ff5b8f8680c2f8232a43f3f52824f97a23d575ab7e5f414b2 |

固定审计器`audit_fixed_v1.py`的完整SHA为`83d99a86ab65901617c907386ca9bfb4927e5e2804153910f301da5cd33523f9`，导入的既有处理时钟比较器完整SHA为`d3a2a632a2a9d6b7cacfd63d21da03cfae6162b39b1d37eefc27513639f2ff33`，审计前后均核验。原始首轮反例和失败证据完整保留。

修复后逐日完整CLI为1064.1075秒；批量checkpoint为683.0676秒、resume为867.4574秒、exact retry为403.4922秒。批量恢复包含两次来源检查，不能与不中断的逐日耗时直接比较；exact retry虽不计算账户，仍有完整来源和输入核验成本。进程树峰值RSS为1712764 KiB，采样峰值FD为43；任务结束采样为0，Reader本身零持有映射由共享交付合同与定向回归证明，不将进程树FD冒充Reader句柄数。两份研究各122个文件、50674084逻辑字节；这是文件长度之和，未宣称物理分配量或冷盘指标。

5份受保护旧产物SHA全部不变；归档时原181日研究仍运行，完成2343/2353个账户日、完整推进到09-29。本次不迁移、不重算或重新验收该旧研究；它仍绑定旧源码及旧身份实现。后续独立重算或跨资源复现需使用正式的新冻结研究，不能把这次通过追溯为旧研究已修复。没有数据库、数据集、行情网络、生产账户或用户服务操作。

## DESIGN-COMPLIANCE-001和运行态

1. 实现绑定真实因果输入，真实三日完整人口、换仓、失败前缀、恢复和exact retry均有收据；首轮差异保留，不将短窗口外推为181日全部重新验收。
2. 不补价格、不吞必要输入错误，不放宽UNKNOWN/NONE、停牌或交易资格；篡改日报仍失败。
3. 不改择时核心、规则、比较器、选股、费用、撮合或旧TWAP政策；只修正LocalSIM研究运行身份。
4. 不新增人工审批；不绕过源码、恢复和运行态门禁。新增源码只在新冻结离线研究加载，用户服务重启仍由用户执行。

源码修复提交完成，离线新进程加载及账户验收通过；PR、最终HEAD CI及合入尚未完成，后端加载和生产激活未操作，经济有效性未评价。工作流仍保守推断backend-main，但catalog未提供对应runbook、identity及business smoke引用；该既有门禁阻断PR readiness。真实离线fresh-process证据只证明新CLI加载修复，不冒充后端业务验收；任何非运行时归类须由流程owner按正式调用闭包/目录合同审查，不手改降级。具体交接为`X:/AIstock_temp/localsim-batch-reader-acceptance-20261028/runtime-catalog-owner-handoff.md`。技术验收不证明人工方式或任何策略有超额收益。

已执行F2设计验证；F-014因上述PR/CI与目录合同未完成保持blocked，整体验证不通过。未为通过解析器虚构用户批准、删除未完成验收项或把离线通过标为源码已合入。
