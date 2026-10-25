# LocalSIM 分钟择时接入与冻结历史账户验证报告

日期：2026-10-24，更新于2026-10-26；范围：F2功能及独立离线研究；PR：[草稿 #6659](https://github.com/licong01-cloud/AIstock/pull/6659)。

## 1. 分项交付状态

| 项目 | 当前结论 |
|---|---|
| 源码接入 | 唯一择时SDK、连续账户及受控后继适配已实现；提交23a589b7的CI通过1551项、跳过22项；实际producer按产物分别固定 |
| 原288场景 | 已完成真实冻结输入下的账户模拟和原比较器验证；本轮只读重试通过，未重算 |
| 年初12账户 | 停牌修复及正式后继绑定已通过；原12账户正在接续。03:11北京时间快照为82个完整日、990/2172账户日，已通过05-08与05-11；实时进度以原run日回执为准 |
| 合入 | F-010/F-013等待完整181日、独立经济核对及后继exact retry；PR #6659保留草稿，未合入，工作树/分支保留 |
| 运行态激活 | 未创建生产账户、未启动生产回放、未修改旧TWAP配置；离线工作无需重启 |
| 经济效果 | 原288样本的描述性比较保留；181日完整结果尚未生成，不用79日替代人口或宣称经济有效 |

原288场景producer commit固定为 `409f3ecf3f0d539415ca9e0e478a9cf452ed9e8b`，release为 `localsim_position_timing_v1-8c5a973d7179e3cd511e0498`。
原源码保留于研究归档tag `research/localsim-position-timing-20261024-r1`；它不是生产发布或激活。
前轮consumer源码、CI和外部证据保留于 `X:/AIstock_temp/localsim-position-timing/delivery-receipt-r2.json`，不能改写原producer身份；本轮共享价格消费与连续研究按以下新增回执分别报告。

## 2. 文件及实现边界

新增/修改源码：`localsim/runtime.py`、`planning.py`、`timing.py`、`timing_store.py`、`timing_research.py`、`timing_campaign.py`、`timing_portfolio.py`、`timing_continuation.py`。
测试：`backend/tests/simulation_execution/test_localsim_position_timing.py`、`test_localsim_timing_campaign.py`，以及既有 `test_twap_policy_authority.py` 的直接验证。
设计：本任务F2详细设计、两份LocalSIM上位合同和本报告；完整精确写入范围见F2设计，不修改其他owner模块。

执行策略是显式opt-in `localsim_position_timing_v1 / POSITION_TIMING_INTRADAY_V1`。旧TWAP默认、参数、历史release和现有账户不变。
原生 `TargetPositionEngine` 和 `RebalanceIntentService` 负责目标及数量，唯一 `position_timing.intraday.evaluate` 负责择时；OMS、MinuteExecutionEngine、InMemoryLedger、父订单累计费用及经济coordinator保持唯一实现。未复制选股、价格触发、过滤或账户算法。

## 3. 冻结输入、账户与费用

- request/bundle：`1c684e78b94872878608cafc48a7a496540b9a9c8cdf62424d886154398d2062`。
- bundle manifest canonical SHA：`8b9128a33c15464c0784b953d658d0666fa2697608e46b073f3a3691ae81b9c1`。
- candidate manifest完整文件SHA：`f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed`。
- 费用完整文件SHA：`d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041`。
- execution contract SHA：`66c8043af0aa34c84bcfb6054ae40d6a4aa23ec291c3c287b4dc1ec76566de5e`。
- LocalSIM账户request完整文件SHA：`3295f1b4c3a149902c2238dc3c69c7830caeca9d7b38c6e485923821ada1ada0`。

24个原计划各有共同初态，分别独立分叉到三规则×四档延迟；全部从1000万元起始现金出发。SELL初态通过同candidate前一交易日真实LocalSIM买入1000股、产生费用回执并经T+1形成，未直接制造持仓。12份准备回执共86.67元费用单独披露，不混入研究日比较。四个研究日不串成连续复利账户。

费用为买0.000095、卖0.000595、最低5元、PER_PARENT_ORDER_CUMULATIVE，是LocalSIM合计费率最低额模型。研究日有 215 个费用为正的场景，其中 69 个触发最低5元，约 32.09%；不能忽略最低费用影响。

`NEXT_ELIGIBLE_BAR_OPEN / strict-after`，0/60/180/300秒，主要观察60秒，allow_split=false，终点14:57。信号仅消费当时完成且可见的分钟前缀；执行开盘与该bar后续OHLCV隔离。分钟区间volume只作为披露的事后容量代理，不是开盘队列成交证明，不模拟市场冲击。

## 4. 原288场景结果与全部未成交原因

人口保持000001.SZ、600000.SH、688981.SH，2026-09-21/22/23/24，各BUY/SELL1000股，24×3×4=288。
FILLED=215、PARTIAL=0、NO_FILL=73、UNAVAILABLE=0；没有丢掉未成交场景。

| 原因 | 数量 | 解释 |
|---|---:|---|
| REMAINING_UNFILLED:NO_ELIGIBLE_EXECUTION_BEFORE_TERMINAL | 53 | 冻结机会在等待期间失效/取消，未获得有效后续执行槽 |
| NO_OPPORTUNITY:SESSION_BREAK,WAITING_FOR_BAR,WAITING_FOR_PRICE | 20 | 原人口仍保留并评价现金或剩余持仓终值 |

215个可执行槽在本人口均产生真实模拟成交；这个观察不能外推为“有槽必然成交”。73个NO_FILL均保留原现金或持仓终值，不将未知填成零收益。

## 5. BUY/SELL/ALL、三规则×四档延迟

下表为等权口径；费用是元/场景。净增量bp是相对同延迟matched B0的终值差除以初始NAV再乘10000；不是收益率百分点。条件价差仅在双方均成交的配对子集中计算；等待也只统计实际成交。MDD降低bp=(B0 MDD-本规则MDD)×10000。未成交仍计入适用分母。

| 规则 | 延迟秒 | 方向 | 完成/适用 | 完成率 | 等待秒 | 费用元/场景 | 净增量bp | 条件价差bp | MDD降低bp |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 简单执行 | 0 | ALL | 24/24 | 100.00% | 50.00000 | 17.80417 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 0 | BUY | 12/12 | 100.00% | 50.00000 | 7.21500 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 0 | SELL | 12/12 | 100.00% | 50.00000 | 28.39333 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 60 | ALL | 24/24 | 100.00% | 110.00000 | 17.78833 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 60 | BUY | 12/12 | 100.00% | 110.00000 | 7.21000 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 60 | SELL | 12/12 | 100.00% | 110.00000 | 28.36667 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 180 | ALL | 24/24 | 100.00% | 230.00000 | 17.80042 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 180 | BUY | 12/12 | 100.00% | 230.00000 | 7.21417 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 180 | SELL | 12/12 | 100.00% | 230.00000 | 28.38667 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 300 | ALL | 24/24 | 100.00% | 350.00000 | 17.78583 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 300 | BUY | 12/12 | 100.00% | 350.00000 | 7.20917 | 0.00000 | 0.00000 | 0.00000 |
| 简单执行 | 300 | SELL | 12/12 | 100.00% | 350.00000 | 28.36250 | 0.00000 | 0.00000 | 0.00000 |
| 到价执行 | 0 | ALL | 19/24 | 79.17% | 50.00000 | 10.97542 | -0.00694 | 9.53049 | -0.06313 |
| 到价执行 | 0 | BUY | 12/12 | 100.00% | 50.00000 | 7.18500 | 0.30753 | 25.06043 | 0.38413 |
| 到价执行 | 0 | SELL | 7/12 | 58.33% | 50.00000 | 14.76583 | -0.32141 | -17.09225 | -0.51040 |
| 到价执行 | 60 | ALL | 13/24 | 54.17% | 110.00000 | 9.10000 | 0.02951 | 3.47308 | 0.04906 |
| 到价执行 | 60 | BUY | 7/12 | 58.33% | 110.00000 | 4.01000 | 0.32653 | 29.42699 | 0.51163 |
| 到价执行 | 60 | SELL | 6/12 | 50.00% | 110.00000 | 14.19000 | -0.26751 | -26.80648 | -0.41350 |
| 到价执行 | 180 | ALL | 7/24 | 29.17% | 230.00000 | 4.65542 | 0.04647 | 19.39685 | 0.07304 |
| 到价执行 | 180 | BUY | 4/12 | 33.33% | 230.00000 | 2.21500 | 0.37583 | 44.69587 | 0.60940 |
| 到价执行 | 180 | SELL | 3/12 | 25.00% | 230.00000 | 7.09583 | -0.28290 | -14.33517 | -0.46331 |
| 到价执行 | 300 | ALL | 2/24 | 8.33% | 350.00000 | 0.43167 | 0.01568 | 43.70629 | 0.09448 |
| 到价执行 | 300 | BUY | 1/12 | 8.33% | 350.00000 | 0.41667 | 0.33513 | 87.41259 | 0.74426 |
| 到价执行 | 300 | SELL | 1/12 | 8.33% | 350.00000 | 0.44667 | -0.30377 | 0.00000 | -0.55530 |
| 有限等待 | 0 | ALL | 24/24 | 100.00% | 50.00000 | 17.76500 | -0.00038 | -0.45477 | -0.09450 |
| 有限等待 | 0 | BUY | 12/12 | 100.00% | 50.00000 | 7.20333 | 0.11168 | 7.09684 | 0.05947 |
| 有限等待 | 0 | SELL | 12/12 | 100.00% | 50.00000 | 28.32667 | -0.11244 | -8.00639 | -0.24847 |
| 有限等待 | 60 | ALL | 18/24 | 75.00% | 110.00000 | 16.47292 | 0.00090 | -5.27597 | -0.05170 |
| 有限等待 | 60 | BUY | 8/12 | 66.67% | 110.00000 | 5.53417 | 0.10251 | 3.83128 | 0.08001 |
| 有限等待 | 60 | SELL | 10/12 | 83.33% | 110.00000 | 27.41167 | -0.10070 | -12.56178 | -0.18341 |
| 有限等待 | 180 | ALL | 18/24 | 75.00% | 230.00000 | 16.50917 | 0.00004 | -1.08746 | -0.06037 |
| 有限等待 | 180 | BUY | 8/12 | 66.67% | 230.00000 | 5.54417 | 0.02667 | -1.53941 | 0.05430 |
| 有限等待 | 180 | SELL | 10/12 | 83.33% | 230.00000 | 27.47417 | -0.02659 | -0.72590 | -0.17503 |
| 有限等待 | 300 | ALL | 18/24 | 75.00% | 350.00000 | 16.50083 | -0.00079 | -4.51417 | -0.04824 |
| 有限等待 | 300 | BUY | 8/12 | 66.67% | 350.00000 | 5.54167 | 0.00917 | -5.53889 | 0.04376 |
| 有限等待 | 300 | SELL | 10/12 | 83.33% | 350.00000 | 27.46000 | -0.01075 | -3.69440 | -0.14024 |

60秒主要口径中，简单执行24/24，到价执行13/24，有限等待18/24。BUY/SELL分项方向并不一致，整体净值差很小；工程样本不构成收益有效性证明。原比较器报告 `DESCRIPTIVE_ONLY_NO_AUTOMATIC_PROMOTION`。

## 6. 性能、恢复与结果交付

| 项目 | 耗时 | 峰值RSS | 说明 |
|---|---:|---:|---|
| 原288账户模拟 | 1049.92秒 | 385020KiB | 其中账户计算959.71秒 |
| 原比较器 | 85.42秒 | 见原回执 | 先完整source/bundle inspect再读账户结果 |
| 本轮288只读重试 | 107.69秒 | 385136KiB | ALREADY_MATERIALIZED，未重算，原SHA不变 |
| 真实01-05原生数量规划 | 41.23秒 | 497004KiB | 50目标、50调仓意图；无OMS订单/成交，不是全年每日回放耗时 |

原研究输出248,903,376字节，约237.37MiB，不含环境、缓存和冻结输入；没有复制大型模型或数据。本轮连续运行实际执行至阻断耗时14,569.20秒（约4小时2分49秒），峰值RSS 1,585,472KiB（约1.51GiB）；79日已提交前缀共2,845个文件、2,526,595,722字节（约2.35GiB）。这些是至5月8日输入阻断的实测，不能当作181日总性能或据此承诺每天40秒。

复杂度审查覆盖原固定研究的三处因果诊断集合，以及连续研究的父订单循环。连续输入最多615个不同股票，按日复用只读行情；终态订单不再推理。组合父订单receipt已移除重复的全组合现金/持仓明细，实际账户与经济事实只在日receipt及coordinator保存；原单计划完整receipt不变。计算量和内存边界见F2设计的ALGO-COMPLEXITY-001段，四处P2提示不等于真实年度性能已验收。

原单计划已做真实已确认成交前缀恢复：恢复后成交数量和费用不重复，完整EconomicRow一致。连续消费者使用完整交易日原子提交；合成功能验证在第3个账户前中断，已提交2个账户日保留，恢复只计算剩余22个账户日。整批source inspect完成后才读取结果；丢失的日摘要可由已提交coordinator journal重建，不重新成交。exact retry复用相同不可变结果；未完成运行拒绝跨source继续。

原结果文件：
`X:/AIstock_temp/localsim-position-timing/study/8c5a973d7179e3cd511e049898c88217a2089d41a4985cdef3987aaa9e5d19e8/economic_results.json`

完整文件SHA：`542bf38129337583b306aa1802b6158b1f4dd52b96e864333157cb13d9c10815`。
比较器结果：`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/intraday_v1/research_studies/comparisons/3e413094126fe7c6b07dbfa316efca3ce1e50f208bfd59ee97085fc854df254a.json`，完整文件SHA `67332a50518a34c6706b3af2d812a766ec3359c088719ac7ae15e9adf50d99d9`。

可交回择时窗口的原比较命令（WSL）：

```bash
python -m backend.services.position_timing.intraday.research compare \
  --bundle /mnt/f/Dev/AIstock_model_artifacts/position_timing_advice_v1/intraday_v1/research_studies/bundles/1c684e78b94872878608cafc48a7a496540b9a9c8cdf62424d886154398d2062 \
  --results /mnt/x/AIstock_temp/localsim-position-timing/study/8c5a973d7179e3cd511e049898c88217a2089d41a4985cdef3987aaa9e5d19e8/economic_results.json \
  --results-sha256 542bf38129337583b306aa1802b6158b1f4dd52b96e864333157cb13d9c10815
```

## 7. 年初连续账户：原阻断历史快照（当前接续状态见第10节）

### 7.1 冻结身份与接口验收

选择最新加入的LightGBM＋12因子，manifest `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`。15个唯一资产blob已验证，没有重训、替换处理器或复制权重；原500万元生产账户未修改。冻结193日原生选股中的181日（01-05至09-30），共9050个目标；10月数据不属于本candidate范围，不静默切换数据库。

原preflight完整SHA `c7270eabf6830df970d4207d25bae9661e5fab464a0e70f5a65cba78f6cc8ff7`，路径 `X:/AIstock_temp/localsim-position-timing/campaign/ece80148db48366428ee8f7b8df78ef12c0e0df0422fc5bfb6932ead3ba9ce5c/preflight.json`。其历史状态BLOCKED_PLAN_INPUT_CONTRACT未被手改。PR #6671返回后另冻新请求，不覆盖旧产物。

共享根目录：`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/intraday_v1/shared_price_plans/2d3cc6547769b19d4d7cf9d6161f3cf0c11eb00feb1684d0806bf5e3026d5167`。

| 文件 | 完整文件SHA256 |
|---|---|
| templates.json | bce2563f61ee2c3ee64b74d723dfac57baa95e127ed87e137229e86d16b327a1 |
| policy_document.json | 58e2fddf837f890e7eecadeef08e2907273f7edc3ab9759d62ae51e892e5b779 |
| reference_audit.json | fe6b1fcb12ffb868a68e95f3e122bbe7969fed533a017afabd3ae23bd02bfde6 |
| manifest.json | 4781d4fa7c27dd7f7c632090c32e87fb17a7d74dba225f607bcd638ddd2323bd |

共享政策RAW_PREVCLOSE_PREFERRED_50BPS_V1经用户授权用于独立探索；采用owner冻结的raw昨日参考价、±0.5%偏好及0.01价格tick，不复制构造算法、不改核心参数。它不是模型价格预测或已获经济验证的生产默认。09:00历史可用性是政策假设，不冒称权威发布时间已核实。

生产者原生inspect和LocalSIM整批来源核验均通过：181日、9050 BUY、64947 SELL，73983 AVAILABLE及14 UNKNOWN。14条审计未知为002743.SZ/09-07一条，以及688496.SH/09-11至09-30十三条，均为备用SELL。声明按日期、股票、方向、类型化原因及来源FileRef精确匹配，不在代码硬编码白名单。B0/C2允许合法NONE；C1在实际原生意图需要缺价模板时保留UNAVAILABLE和原因，不发订单、不补价。未使用备用模板不阻断账户日；行情和估值真缺价仍失败关闭。C1混合修订含UNKNOWN的未来合同不在本冻结单ACTIVE研究范围，当前仍拒绝。

整批双端检查：`shared-price-inspection-r4.json`，完整SHA `cb12d3c921830662e2110cec15dfb1b5fb14cba321029ff21f6929c3111d13fa`；197.89秒、峰值764088KiB。这是来源/模板检查，不能证明未来实际所需的每根分钟记录完整，也不是每日账户回放性能。

实际冻结时另发现本消费侧把原生现金字符串10000000.00与10000000直接比较的问题，已改为Decimal金额比较，四个新增断言通过。未重写初始账户、来源回执或已冻结输入。完整直接验证为78个不同节点（32单计划＋44连续＋2旧TWAP）。代码producer为 `e6f66f07a7b10418b564b04beef8ae42c89add78`；其[CI 40071720197](https://github.com/licong01-cloud/AIstock/actions/runs/40071720197)通过1510项、跳过22项，无DEV数据库验证要求。代码CI收据为 `ci-receipt-r3.json`，完整SHA `8a2ad1760a215410c9dc92cee3f141df3f9ebf9c88200ca630e4af481cd9255b`；后续文档提交不重写原producer身份。

### 7.2 实际连续运行与失败归因

12个独立离线账户均从纯现金1000万元开始，三规则×0/60/180/300秒。原始9050目标、初态、费用及14:57共同终点不变；数量由原生目标/调仓服务按各账户真实反馈计算。成交分化后持仓、净值和后续数量会分化，年度比较为COMMON_EXOGENOUS_TARGETS_FEEDBACK_DIVERGENT_QUANTITIES，不冒充原288共同计划的纯延迟效应。

新请求：`X:/AIstock_temp/localsim-position-timing/continuous/92bcea72a816607b478c23324b9578ad04b96bd7303a1af95a9f215cbb36e1d3/continuous_request.json`；完整SHA `a9ab2c0388702fbe746061b466083706d189dd2f7d4f29a6c4fba6c7a0a14668`。release为 `localsim_timing_campaign_v1_92bcea72a816607b478c2332`；执行合同SHA `c4d4f42a92f589adb50b3ee46e464314e3da8c901e93b244bef4d8d31be2a5d8`。12份初始账户FileRef保存在该请求及交付收据中。

已完成2026-01-05至05-07的79个交易日，12账户均为79日，合计948个原子账户日；剩余102日、1224账户日尚未验收。05-08装载实际所需的98只股票时，97只通过，唯一失败对象为601010.SH。它已退出当日50目标，但仍是PRICE_TOUCH_V1＋300秒账户的真实持仓，不能因不在新目标或PIT选股范围就删除存量持仓。

该股票05-08的11:30结束分钟（11:29—11:30）在冻结candidate中，open/high/low/close/volume/amount/factor均为NaN；按现有可见合同11:30:05已可见，早于共同终点14:57，不能解释为终点之后未使用的数据。原生入口经过D日交易状态与日级价格校验后，在RESEARCH_MINUTE_VALUES_INCOMPLETE处拒绝读取。当前可确认物理文件缺值；未查询数据库或权威行情源，故不能把更上游根因直接定性为数据库缺失，也未把它改写为审计模板UNKNOWN。

失败日尚未发生订单、成交或账务提交，948个既有日journal与摘要保留不变，没有continuous_results.json。保留全部181日/9050目标和12账户，没有换股、缩期、补价格、删除持仓或回退数据库。未运行年度完整经济比较，也没有把后续未运行账户日伪造为NO_FILL/零收益。

### 7.3 证据、恢复及消费命令

以下文件均位于 `X:/AIstock_temp/localsim-position-timing/`，均为完整文件SHA：

| 收据 | SHA256 | 结论 |
|---|---|---|
| continuous-freeze-r5.json | 401b382b95827e5f077ced6c9821958a5f1782af32a79c6d9e475c49c8b924ee | 新请求正式冻结，旧初态字节不变 |
| continuous-run-r5.json | 7886e69780cfc25bce4ba7aa0606997166373844b15d66f313fef42c31247ff6 | 实际运行至05-08输入阻断 |
| minute-gap-diagnosis-r6.json | c723f8bcd8fe5cd8384c01dd6b1bebe091f7b26fe56b4adcc11053516de328a8 | 98股票逐一调用原生入口，精确字段/分钟/文件SHA和实际持仓账户 |
| real-prefix-validation-r6.json | 5226e4188a777bc091e162acd0612ee42aec58c6f08b2d8a92b5dec577d1b2a4 | 948账户日原生ledger独立复核通过，207.28秒 |
| blocked-delivery-r6.json | e4667dbf36e6cc4813876c5d36ce18c36e834f7d73911548a2e3f3fb327a2b08 | 接口/回放/经济状态分开绑定，含初态FileRef与前缀空间 |

复核包含journal hash、摘要前缀链接、跨日现金、持仓/T+1、原生成交/费用账本重放及现金非负。它只验收948日已确认前缀，不替代181日整批验收。完整结果尚不存在，年度completed-result exact retry不适用；原288结果只读exact retry仍为ALREADY_MATERIALIZED，完整SHA已重新确认不变。原288的FILLED=215、NO_FILL=73、PARTIAL/UNAVAILABLE=0及既有36组比较保留。

正式运行入口保持不变，以下不是本轮额外重跑成功记录；输入缺口解决且身份合同闭合后才能接续：

```bash
python -m backend.services.simulation_execution.localsim.timing_campaign run \
  --request /mnt/x/AIstock_temp/localsim-position-timing/continuous/92bcea72a816607b478c23324b9578ad04b96bd7303a1af95a9f215cbb36e1d3/continuous_request.json \
  --request-sha256 a9ab2c0388702fbe746061b466083706d189dd2f7d4f29a6c4fba6c7a0a14668
```

不能原位修复已冻结candidate后沿用旧SHA，也不能静默换active profile。若需新candidate或共享计划身份，须由相应owner交付正式版本及兼容/迁移合同，LocalSIM再核验已提交前缀的受控接续；本轮没有改请求、断点、账户或失败状态来绕过校验。

### 7.4 可直接交给数据准备窗口的提示词

> 请只读核查冻结candidate（完整manifest SHA `f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed`）中601010.SH／2026-05-08的11:30结束分钟。其`components/minute_bin_candidate/features/601010.sh/`下open/high/low/close/volume/amount/factor七个1min.bin在该位置均为NaN，导致唯一原生CandidateReader报RESEARCH_MINUTE_VALUES_INCOMPLETE。精确绝对路径、七个完整文件SHA及上下文见 `X:/AIstock_temp/localsim-position-timing/minute-gap-diagnosis-r6.json`（完整SHA `c723f8bcd8fe5cd8384c01dd6b1bebe091f7b26fe56b4adcc11053516de328a8`）。请核对权威源、分钟时标、成交/停牌状态和导出链，区分实际缺数、导出遗漏及合法无成交。它是C1＋300秒账户的真实存量持仓，不是当日候选，也不是14条备用SELL模板UNKNOWN；不能按PIT删除持仓或造bar/补价。若需修复，请返回不可变的新版本候选、manifest/组件完整SHA、影响范围和独立读回；不覆盖原f5版本、不擅自激活或用数据库替代本研究。若权威源证实合法无成交而需读取合同变化，请明确交由择时/数据入口owner处理，不要求LocalSIM放宽真缺价。新candidate若改变冻结共享计划身份，须同时交付正式重绑定/迁移合同，禁止静默替换。12账户已完成79日、948个经济journal，05-08写入为零，全部前缀必须保留；本轮未操作生产DB或服务。

## 8. 多轮审核、回归及边界

前一阶段59个定向节点已验证通过（文件分布：{'test_localsim_position_timing': 32, 'test_localsim_timing_campaign': 25, 'test_twap_policy_authority': 2}）。主轮55通过，新增恢复隔离断言的测试导入类名错误导致4个节点失败；纠正后仅重跑该4节点，全部通过，未重复整组。原始XML、失败节点复跑和最终源码文件SHA汇总于 `X:/AIstock_temp/localsim-position-timing/direct-validation-r3.json`；本地pytest metrics未设置HEAD字段，不冒称提交绑定证据，最终提交由CI收据绑定。资金/持仓不变量、取消、部分成交、T+1、方向性限制、四延迟、来源完整性、未来信息隔离、12账户×2日恢复及原生账本独立读回均有直接验证。旧TWAP定向2项通过。更大范围回归交由PR CI，以当前HEAD的稳定CI verdict和紧凑收据为准。

本轮新增审计UNKNOWN/NONE来源与人口审核，以及实际C1不可用、B0/C2确认、无订单日提交和exact retry审核；74个稳定节点通过后，原生现金格式修复另验证4个不同节点，总计78个。代码未因05-08真实缺值而放宽，完整F2仍明确BLOCKED；最终文档HEAD的CI以PR稳定CI verdict读回为准。

审核第一轮修复部分成交确认时钟：等待available_at后才能继续使用该确认推理/执行；共享现金反馈也更新真实可见时间。第二轮修复普通股票分钟缺口被当作终值旧价的问题，并保留原生规则拦截请求的统计分母。第三轮核对日原子恢复、全部成交/费用/持仓经原生ledger重新读回，以及exact retry不重复成交。设计与报告中文乱码已修复。

第四轮修正既往持仓退出模板的最小要求：离开当日目标后只要求SELL，不强制BUY；缺实际需要的当日BUY或历史SELL仍阻断。组合父订单改为独立审计片段，避免重复全组合账本，且验证其不能冒充完整账户恢复输入。真实首日数量规划和288只读重试的原回执保留当时源码身份；这两项未因后续消费者修正而重算，不把旧回执改写成最终HEAD的实测性能。

Windows临时文件和依赖均在X盘；WSL直接读取F/X挂载。只在本任务隔离WSL环境补齐仓库已声明的jsonschema/aiofiles导入依赖，未更改生产环境。未查询生产数据库、执行DDL/DML、修改数据集、访问行情网络或控制backend/worker/scheduler；真实文件验证用审计钩子拒绝socket连接、subprocess和任务输出范围外写入。

离线工作不需要用户重启。未来生产启用仍须完整immutable release/control-plane接入及独立授权，用户负责后端重启；不能仅凭此离线policy或CI通过直接运行生产账户。

## 9. DESIGN-COMPLIANCE-001逐项检查

1. 禁止简化交付：接入、原288模拟、12账户真实连续研究分别报告；数据停牌事实修复及后继绑定已通过，但完整181日、独立核对及后继重试仍在执行，不声称全部完成、缩期经济有效或请求合入。
2. 禁止静默错误：价格/数据/身份缺口显式阻断；未成交不删行，未知不填零；恢复保留已确认前缀。
3. 禁止改变业务逻辑：核心参数、原288人口和费用、旧TWAP/release/账户不改；年初连续研究明确属于用户追加范围和独立闭环口径。
4. 禁止私增审批：遵循既有跨owner边界和完整F2验收，不新增审批、不重复请求已有合入授权；满足全范围条件后可直接合入和本任务清理。

## 10. 2026-10-26：停牌修复后的受控接续

本节覆盖第7节的历史阻断归因，不改写旧回执。BUG-2089/2090已修复并正式激活；601010.SH/2026-05-08和05-11是原生权威可解析的全天停牌，原分钟NaN无需补造。根因是旧导出按买入PIT裁剪停牌事实，持仓退出买入池后未能消费其停牌状态。新候选manifest完整文件SHA为`3d416575b4dbb3cdf6d17552908c38fcc03c96f4fe4b12fa30c6ee4d1db6a36a`；原`f5af...`版本、原共享价格计划和948个账户日前缀均保留。

整批先inspect后读取前缀，独立可行性回执`X:/AIstock_temp/localsim-position-timing/successor-feasibility-r8.json`，完整SHA `2b00f06d8dbb95a14a51ae0c22c6a48f277c7be48455430512dc1559f11607c3`：615股票的6152个日历/日线/分钟行情组件pin全部相同，报价范围相同，615个价格偏好原始来源全部相同；6926个实际目标/既有持仓市场键的前缀停牌状态变化为0；948份summary及948份journal均与旧独立账本证明一致。耗时394.64秒。这不是全数据集等价声明：其他证券确有日价、分钟及因子修复，不能静默换成active。

新增LocalSIM专属`timing_continuation.py`和显式`adopt-successor`入口；不修改共享核心或数据模块。原run目录只追加不可变`input_successor.json`，绑定旧请求、新candidate、原模板/费用/执行政策、源码、原生前缀实体引用。后续日身份追加该FileRef，摘要及最终结果明确记载后继；旧948日逐字节保留。先整批source检查，随后验证原生journal/summary、跨日现金/持仓/T+1和实际市场输入；任何前缀不兼容拒绝adopt，不重做旧交易，不手改断点。源码迁移仅允许campaign编排适配，冻结撮合/账本/数量/费用/择时核心仍逐文件保持一致。

两轮审核分别核对账户/源码隔离与恢复/来源漂移。后者增加封存前来源及前缀再读回，避免长检查期间变动；输入读取器、公共停牌authority及新增接续模块也绑定实际源码。首轮两个漂移测试预期异常类型错误，实际共享读取器已正确拒绝；仅重跑该两节点，2项通过。三个直接文件矩阵`direct-final-r10.xml`共92项通过（99.47秒），覆盖原TWAP、原288适配不变量、12账户后继接续、缺摘要原生恢复、exact retry和不兼容/漂移拒绝。随后恢复审查补齐后继绑定后旧前缀摘要缺失的情况：只能从已绑定原生journal重建，生成字节必须符合冻结hash；已有损坏摘要不覆盖。该修复的5个定向节点`direct-prefix-recovery-r11.xml`全部通过（66.95秒），包括旧/新日摘要恢复、数据/前缀/源码漂移拒绝及原未迁移恢复；最终全模块覆盖以修正版HEAD的CI为准，不将前一源码92项冒称为新源码的完整重跑。L0四文件1项已审查复杂度P2、0阻断；所有权4/4映射，0歧义。

正式adopt已完成：`successor-adopt-r11.json`完整SHA为`680075fa86599135135c708312643aba61b7cd17d18d1c2700a38f9b49d8b7fb`，状态ADOPTED，耗时749.83秒、峰值881096KiB。原run追加的`input_successor.json`完整SHA为`00e95019b12fcd11dd6098486658b08376292acd4bef7dd9e795543103c366df`；release为`localsim_timing_successor_v1_34209f1742596d0c01207484`，实际接续producer为`23a589b7b9f6da5b34e952da157b1f50f5ef8348`。正式核验包括12个PIT组件，原政策及全部价格计划未迁移；948个原生账户日保持原身份和字节，接续日为2026-05-08。

该修正版HEAD的[CI 40221704559](https://github.com/licong01-cloud/AIstock/actions/runs/40221704559)稳定verdict为SUCCESS：81＋1407＋63＝1551项通过、21＋1＝22项跳过。紧凑回执`ci-successor-r11.json`完整SHA为`e27763e22586b171153334c594bb505282fa4b938bdc63648a7260546db0cf03`。原288结果与比较文件重新核验SHA均未变化；保留回执`original-288-preservation-r11.json`完整SHA为`132f53d514436b75637f635535c7094aaa2673163e28e2172ced45bbe59082fe`，未重算原研究。

同一12账户已正式启动剩余102日离线接续，不是新建研究来绕过失败。2026-10-26 03:11北京时间元数据快照为990/2172账户日、82个完整交易日，05-08与05-11均12账户完成，最新运行日05-13。此为进展快照；最终完整结果、2172日独立账本核对、36组比较和后继exact retry仍待完成，F-010/F-013未声明PASS，PR保留草稿。全部真实操作在WSL使用冻结F/X文件；无生产DB、数据集改写、行情网络或服务控制，离线接续无需用户重启。

阶段性能只读收据`continuous-progress-measurement-r13.json`完整SHA为`169f2b24360e06154297d552c6e206f1eac07e5118ace8437f59269b201f7fb1`。测量时89个完整交易日、1073个账户日；本次接续已完成交易日的相邻提交间隔中位255.92秒、均值243.09秒，口径是12账户合计完成一个完整交易日，首日启动及来源检查另计。阶段累计2883.58秒、CPU2397.41秒、峰值1673600KiB。该收据只读取提交路径元数据及本任务进程状态，未读取经济结果；不是181日总性能，也不预设后续日期耗时相同。
