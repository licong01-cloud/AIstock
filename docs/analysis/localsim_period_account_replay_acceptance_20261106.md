# LocalSIM 10月独立期间账户回放验收

日期：2026-11-06。设计：`docs/architecture/localsim_period_account_replay_f2_20261106.md`。

## 当前验收状态

源码接入、本地门禁、真实17日/13户/221账户日、正式inspect及本轮一次完整经济exact retry均已完成。重算221账户日、经济事实完全相等、新增提交0日。本报告记录提交PR前的技术验收；最终HEAD的CI与合入是后续独立门禁，结果由GitHub及外部`feature-merge-receipt.json`记录，不用本地验收代替CI或经济有效证明。

输入契约PR #7048已合入`e017bac21321b43800770d6fdfd8b4a9a79b5acd`。历史事件时钟BUG-2186 / Issue #7065已由PR #7068合入`112c682a8935f9d4fd41dfa68bfd33e209e13f1a`；该PR CI run `42160592380`成功。后端加载尚未验收，close-sync PR #7069保持OPEN，不等待或控制服务。

真实研究生产者commit：`bcc15ee5f52584523dc1d07793764b8152d65ebe`。独立交付工作树以主线为基线，只接功能commit，初始实现commit `d5984ebf02daa253ca85dd27642d9a63f101c67f`；同步`070ae474f8e1663879e0c109ca8861540d86a5f3`后为`cf80279624f38e55d8b63fac79ca88107e86660c`。57份消费源码指纹与生产者完全一致，证据为本任务外部目录`delivery-source-equivalence-r2.json`；运行中生产者工作树未修改。

## 实现与合同

- `localsim/period_research.py`：独立v2期间request、freeze/run/inspect/economic-retry、不可变results/facts/spec及receipt-last发布。
- `localsim/dual_mode_research.py`：提取既有账户日编排，继续调用唯一`_compute_account_day`和原生账户、费用、成交、交易约束及事务链；默认旧v1及TWAP业务保留。源码升级仍按原合同改变源码指纹，不在新版下恢复旧源码研究。
- `localsim/timing_campaign.py`：v2历史持仓SELL种子、合法省略零值的审计Counter消费，以及Reader finally关闭。未知类别、数值、未声明UNKNOWN和来源冲突仍拒绝。
- 已合入的BUG-2186修复只在明确离线适配能力下，把成交事件时间绑定真实fill.trade_time，把取消事件绑定实际确认clock并生成确定性取消ID。原成交数量、价格、费用及通用OMS不变；默认取消调用保留原行为。

所有账户继承各自9月末现金、持仓、成本、可卖量和分析锚点。1000万元是原始资金原点，不是10月重新入金。股票按既有原生投影编译目标；无重新选股、训练、换股、调价或费用调整。

旧1—9月181日/13户/2353账户日产物、原288案例及失败证据均保留；本次仅10月8—30日17日、850原生目标。包含73只历史持仓及12只prior-only SELL的最终模板3174条，BUY850/SELL2324/UNKNOWN0，股票全集182只。原18条source-quality警告和7条历史volume差异披露保留；不把后者称为完整父回放等价。

经济比较仍为三规则×四延迟与native基线，主要延迟60秒、TTL90秒、14:57共同终点、allow_split=false、原费用不变。费用严格绑定冻结文件：买入0.000095、卖出0.000595、父订单累计最低5元；这是LocalSIM合计费率最低额模型。系统审计created_at/updated_at依原生typed fact合同规范化；state_hash先原生验证，再对canonical state fact比较摘要；不删除事件时间、因果时钟、价格、数量、费用、NAV或反馈，不采用数值容差。原始审计实体及hash全部保留。

## 冻结输入与结果身份

外部任务根目录：`X:/AIstock_temp/localsim-october-account-replay-20261106`，WSL为`/mnt/x/AIstock_temp/localsim-october-account-replay-20261106`。

| 项目 | 完整文件SHA / canonical身份 |
|---|---|
| v2 preflight | `12bdae08351b363f9da871b70e20e1a2faee4caafdad897d7146fb7e61636e56` |
| preparation_request | `5f25528dd2bfc0f1a5f45e3bef2911b22032e95291a3cbe932a43c4f2d81fb54` |
| selection_inventory | `d55b4fff01b79f55c223e35b6f185200410290ce9921468f68b90b6aada6b6fc` |
| 最终templates | `245b88622a6f0a1ea9fc43f5577944bd5826f800961b05b66a2966abbdae7cde` |
| price policy_document | `10519776e781d8d72f44264cb94d6bef843bfa62a2cb15eb9309616ddb5a021a` |
| price manifest | `02089ceec4ffca123e6bab8fa1352c4c0d63f41b619a8a6d722b7a2fe724bd54` |
| candidate manifest | `2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958` |
| 费用文件 | `d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041` |
| execution contract canonical SHA | `e89ae9b5103c0a5cab3fbb9b8bbb86273b8a24cadd3f8e18309b4b0f562d8140` |
| 本研究request | `cf855f806ecc27fa1b38093f3d4f61308f66382f0f85b477fe83830648d6bd8b` |
| results.json | `e77addf51fa50f694eedc22365251550aefb421f6ccee44810d27ab1863dca3b` |
| facts.json | `4528c2d52161aa36c8120dd9d361a6e386dbc26572bb12c1c5e1b9295e43f490` |
| analysis_spec.json | `a48db077873ca0cfd1b3494816eda33d2d0adc086bc960a43beb39429ef8df21` |
| 最终source_inspection.json | `00539788912e9a6a2692619eff2e515f8dc17c34cd08057ba18f93655c686015` |
| economic_retry_receipt.json | `43cd1203946bc1d417b60144f8083755d9388fa1f193941f26e17f4351a1bba8` |
| result_receipt.json | `78b0645ceda023a0c2838c441a0d11e1c1959935de5039f7cb156842fd16cf3c` |

研究目录为任务根目录下`studies/63fb7784ae238513c74d7d3bfed86cf10b822573d0ffddff79fddf5703868bd1`。上述结果已materialize、inspect及真实经济retry通过；初次与最终results/facts/spec/source FileRef完全一致。完整绝对路径和SHA已经独立回读写入`X:/AIstock_temp/localsim-october-account-replay-20261106/formal-delivery.json`；源码合入后由同根`LOCAL_SIM_HANDOFF.md`交回择时owner，不重新计算结果。

## 验证与资源

最终定向矩阵：

```text
pytest backend/tests/simulation_execution/test_localsim_period_research.py
       backend/tests/simulation_execution/test_localsim_dual_mode_comparison.py::test_thirteen_accounts_two_days_resume_native_journals_and_exact_retry
       backend/tests/simulation_execution/test_localsim_dual_mode_comparison.py::test_invalid_economic_day_fails_before_any_journal_is_committed -q
```

6项通过，包含原生继承初态、来源先于收益、真实核心调用计数、部分提交恢复、经济差异拒绝、v1非法入口拒绝和旧v1/TWAP回归。BUG定向`test_localsim_historical_cancel_clock.py`另4项通过，覆盖部分成交、确认时钟、取消重试和旧默认。证据为外部根目录`targeted-final.xml`、`bug2186-targeted.xml`、`source-review-r3.json`。三轮顺序审核已完成；Ruff、diff检查和变更范围L0通过。

WSL复用既有环境与网络guard，batch-days=3、account-workers=2、optimized=True，资源参数不进入经济身份。原始source整批核验2670份文件先于本期收益读取；每日日绑定投影不跨日复用。初次回放221账户日耗时2991.33秒（约49.86分钟），包含行情读取、13户执行及提交收尾；inspect耗时91.68秒、重新计算0日。经济retry引擎耗时2960.01秒（约49.33分钟），监督阶段含收据收尾2961.40秒，完整重算221日且新增提交0日。该时长不是每日选股推理耗时，本轮未运行模型推理。

初次行情准备630.07秒、retry行情准备626.35秒，均为相应端到端时长的子集，不能相加。20秒采样的进程树RSS峰值1,627,815,936字节（约1.52 GiB，非操作系统精确高水位），研究目录459,272,040字节（约438.0 MiB，不含共享输入）。保留52,598条NAV观察、5,645个订单、10,197笔确认成交及对应10,197笔现金分录；221份原生journal均为generation=1。此为真实账本事实计数，不把机会、提醒或执行槽计作成交。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-401 | period_research._sources/_inspect_request；timing_campaign.inspect_price_templates | artifact: source-review-r3.json；真实raw_source_inspection.json；2670来源核验及source-before-outcomes测试 | passed | none |
| F-402 | period_research._initials；dual_mode_research.execute_account_days | pytest backend/tests/simulation_execution/test_localsim_period_research.py；真实13户221日journal/native ledger inspect | passed | none |
| F-403 | dual_mode_research.execute_account_days/_compute_account_day；ResearchAccountPool/ResearchMarketWorkspace | artifact: delivery-source-equivalence-r2.json；6项定向回归；真实WSL batch3/workers2 | passed | none |
| F-404 | period_research.run_period/inspect_period；TimingFileRepository | pytest backend/tests/simulation_execution/test_localsim_period_research.py::test_partial_day_prefix_resumes_without_duplicate_economics；差异及旧入口拒绝测试 | passed | none |
| F-405 | dual_mode_research._verify_recomputed_account_day/_economic_account_payload；period_research.run_period | artifact: X:/AIstock_temp/localsim-october-account-replay-20261106/formal-delivery.json；EXACT_RETRY_VERIFIED，重算221日、经济事实一致、新提交0日 | passed | none |
| F-406 | period_research._publish_results/_inspect_completed | artifact: read_only_inspect_receipt.json；真实221日及公共PeriodAnalysisSpec/period_source_preflight通过 | passed | none |
| F-407 | 本报告；外部研究与监督收据 | artifact: X:/AIstock_temp/localsim-october-account-replay-20261106/supervision_complete.json；221日/inspect/经济retry全通过，DB/数据/服务操作false | passed | none |
| F-408 | test_localsim_period_research.py；本报告及F2设计 | artifact: X:/AIstock_temp/localsim-october-account-replay-20261106/source-review-r3.json；10项定向及三轮审核通过，F2/L0为提交前检查，最终HEAD的CI仍为独立必需合入门禁 | ready | none |

## 运行与经济边界

DESIGN-COMPLIANCE-001逐项复核：

1. 人口、期间、冻结输入、账户继承及公开facts/spec完整实现；真实221日、inspect及完整经济retry均已通过，不以子集、缓存或mock冒充全量。
2. 身份、缺失、损坏前缀及经济差异均显式拒绝，失败证据保留；未用默认现金、价格、空结果或缓存命中伪造成功。
3. 三规则、延迟、有效期、费用、目标与账户业务未调整；未覆盖旧TWAP默认、原288案例、1—9月结果或择时owner实现。
4. 仅执行本次授权的来源检查、账户回放、inspect、一次经济retry及正式源码门禁；不增加生产操作、重复重算、等待重启或新的人工审批。

数据库查询/DDL/DML、数据集补写、行情网络、服务进程控制及生产账户操作均未执行。启动时的WSL不能解析Windows worktree Git元数据问题在外部启动脚本解决，改为Windows生成的commit/tree冻结收据加WSL独立文件hash核验；原失败日志保留，未产生账户日或改写冻结输入。

不在LocalSIM内复制择时价格构造、比较器或收益归因。正式facts为`position_timing_period_facts_v1`，提供全部NAV观察及13户17日FileRef；交易、现金和持仓仍由原生journal回读。`economic_confirmation=false`；收益/回撤及10月新增相对表现交给择时owner使用唯一`analyze-period`计算。不得把父账户净值差称为10月新增优势，也不得把原生TWAP称为matched人工B0。

本功能为离线新进程使用；共享历史事件修复的在线加载按BUG-2186既有目录保持待用户重启，不影响当前离线研究，也不在本窗口启停后端。
