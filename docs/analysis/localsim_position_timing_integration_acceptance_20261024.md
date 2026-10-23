# LocalSIM 分钟择时接入与冻结历史账户验证报告

日期：2026-10-24；范围：F2功能及独立离线研究；PR：[草稿 #6659](https://github.com/licong01-cloud/AIstock/pull/6659)。

## 1. 分项交付状态

| 项目 | 当前结论 |
|---|---|
| 源码接入 | 唯一择时SDK、LocalSIM成交确认链已接入；新增连续账户消费CLI，待最终HEAD的CI读回 |
| 原288场景 | 已完成真实冻结输入下的账户模拟和原比较器验证；本轮只读重试通过，未重算 |
| 年初12账户 | 合法初态及181日原生选股已冻结；消费能力和功能恢复验证已完成；缺正式价格偏好输入，真实连续回放未启动 |
| 合入 | 完整F2仍因F-010未完成而失败；PR保留草稿，不把CI通过作为范围完成证明 |
| 运行态激活 | 未创建生产账户、未启动生产回放、未修改旧TWAP配置；离线工作无需重启 |
| 经济效果 | 原样本仅作描述性比较，未证明超额收益，不自动晋升或启用生产 |

原288场景producer commit固定为 `409f3ecf3f0d539415ca9e0e478a9cf452ed9e8b`，release为 `localsim_position_timing_v1-8c5a973d7179e3cd511e0498`。
原源码保留于研究归档tag `research/localsim-position-timing-20261024-r1`；它不是生产发布或激活。
本轮consumer源码、CI和外部证据以PR当前HEAD及 `X:/AIstock_temp/localsim-position-timing/delivery-receipt-r2.json` 的最终绑定为准，不能改写原producer身份。

## 2. 文件及实现边界

新增/修改源码：`localsim/runtime.py`、`planning.py`、`timing.py`、`timing_store.py`、`timing_research.py`、`timing_campaign.py`、`timing_portfolio.py`。
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

原研究输出248,903,376字节，约237.37MiB，不含环境、缓存和冻结输入；没有复制大型模型或数据。全年真实回放尚未运行，端到端耗时、峰值内存及磁盘占用未测量，不以本表预测每天40秒。

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

## 7. 年初连续账户状态及择时owner最小交接

按用户指定的加入顺序选择LightGBM＋12项因子，manifest `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`。原15个唯一资产blob已验证，没有重训、替换处理器或复制权重。原500万元生产账户未修改。

已找到193日原生选股，覆盖2026-01-05至10-23；本冻结candidate只支持其中181日（01-05至09-30）、9050目标，无缺日/冲突。10月12日数据在本次candidate范围外，不称为数据库缺失，不静默回退数据库。9050目标都没有价格偏好区间。

12份纯现金1000万元离线初态已冻结：
`X:/AIstock_temp/localsim-position-timing/campaign/ece80148db48366428ee8f7b8df78ef12c0e0df0422fc5bfb6932ead3ba9ce5c/preflight.json`
完整文件SHA：`c7270eabf6830df970d4207d25bae9661e5fab464a0e70f5a65cba78f6cc8ff7`。
旧preflight仍为 `BLOCKED_PLAN_INPUT_CONTRACT`；没有手改它来伪造恢复或运行。合法新模板返回后，freeze CLI另写新不可变request，再运行12账户。

原研究operator脚本 `research_studies/operator_inputs/20261023_real_smoke/build_inputs.py` 的SHA为 `5d99bd7438878190aaf711c20ff7d4937a71ec610499965a01fbcf71962773bd`，其中0.5% PREFERRED明确属于 `PREDECLARED_MECHANISM_PLAN_NOT_PRODUCTION` 的3股×4日样本，不能自动扩大为策略包原生偏好。

消费schema已经冻结在 `timing_campaign.py::CampaignPriceTemplates / PriceTemplateEvent`，机器schema位于：
`X:/AIstock_temp/localsim-position-timing/campaign-price-templates.schema.json` 和 `campaign-price-event.schema.json`。

可直接转交：

> LocalSIM已交付唯一核心的288场景结果，以及连续12账户消费能力。请为LightGBM＋12因子manifest `5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`，提供01-05至09-30、绑定原candidate完整文件SHA `f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed` 的合法价格偏好合同/冻结模板。优先返回符合上述两份JSON schema的不可变文件和完整文件SHA。外层需绑定producer commit、包/candidate/selection_inventory身份、price_policy_id和policy_document FileRef；policy_document须明确起止日期、CONFIRMED_FEEDBACK_CLOSED_LOOP及适用性说明，明确原0.5%工程政策是否允许扩大。每个日期提供PriceTemplateEvent文件：日期、股票、BUY/SELL、revision/lifecycle、available_at、valid_from/until、source_file及source_available_at、PREFERRED/HARD PriceConstraint；constraint.source_sha256必须绑定原始来源完整文件SHA。除当日目标外，还须覆盖可能因未成交继续持有的既往目标的SELL偏好。不得改原288人口/阈值/核心/比较器。数量、持仓、撮合、费用及全年运行由LocalSIM负责，不要求择时窗口创建账户或执行回放。若选择公共构造器形式，请先返回正式函数签名、冻结政策与适用性/来源/撤销合同，不将业务参考价直接解释成偏好。

连续组合使用共同外生选股、价格模板、初态、费用和数据；真实成交不同后，持仓、净值和后续调仓数量会不同。其结果明确为 `COMMON_EXOGENOUS_TARGETS_FEEDBACK_DIVERGENT_QUANTITIES`，不能混称原288共同计划的纯延迟效应。BUY/SELL分别报告完成、等待、费用和条件成交价差；全账户净值/MDD只在ALL报告，不编造方向净值归因。

owner返回后由LocalSIM执行（尚缺真实参数，不把下列占位符当作成功调用）：

```bash
python -m backend.services.simulation_execution.localsim.timing_campaign inspect --preflight <上文WSL路径> --preflight-sha256 c7270eabf6830df970d4207d25bae9661e5fab464a0e70f5a65cba78f6cc8ff7 --price-templates <owner文件> --price-templates-sha256 <完整文件SHA>
python -m backend.services.simulation_execution.localsim.timing_campaign freeze --preflight <上文WSL路径> --preflight-sha256 c7270eabf6830df970d4207d25bae9661e5fab464a0e70f5a65cba78f6cc8ff7 --price-templates <owner文件> --price-templates-sha256 <完整文件SHA> --output-root /mnt/x/AIstock_temp/localsim-position-timing/continuous --producer-commit <已验证源码commit>
python -m backend.services.simulation_execution.localsim.timing_campaign run --request <freeze回执路径> --request-sha256 <freeze回执完整SHA>
```

## 8. 多轮审核、回归及边界

本轮59个定向节点已验证通过（文件分布：{'test_localsim_position_timing': 32, 'test_localsim_timing_campaign': 25, 'test_twap_policy_authority': 2}）。主轮55通过，新增恢复隔离断言的测试导入类名错误导致4个节点失败；纠正后仅重跑该4节点，全部通过，未重复整组。原始XML、失败节点复跑和最终源码文件SHA汇总于 `X:/AIstock_temp/localsim-position-timing/direct-validation-r3.json`；本地pytest metrics未设置HEAD字段，不冒称提交绑定证据，最终提交由CI收据绑定。资金/持仓不变量、取消、部分成交、T+1、方向性限制、四延迟、来源完整性、未来信息隔离、12账户×2日恢复及原生账本独立读回均有直接验证。旧TWAP定向2项通过。更大范围回归交由PR CI，以当前HEAD的稳定CI verdict和紧凑收据为准。

审核第一轮修复部分成交确认时钟：等待available_at后才能继续使用该确认推理/执行；共享现金反馈也更新真实可见时间。第二轮修复普通股票分钟缺口被当作终值旧价的问题，并保留原生规则拦截请求的统计分母。第三轮核对日原子恢复、全部成交/费用/持仓经原生ledger重新读回，以及exact retry不重复成交。设计与报告中文乱码已修复。

第四轮修正既往持仓退出模板的最小要求：离开当日目标后只要求SELL，不强制BUY；缺实际需要的当日BUY或历史SELL仍阻断。组合父订单改为独立审计片段，避免重复全组合账本，且验证其不能冒充完整账户恢复输入。真实首日数量规划和288只读重试的原回执保留当时源码身份；这两项未因后续消费者修正而重算，不把旧回执改写成最终HEAD的实测性能。

Windows临时文件和依赖均在X盘；WSL直接读取F/X挂载。只在本任务隔离WSL环境补齐仓库已声明的jsonschema/aiofiles导入依赖，未更改生产环境。未查询生产数据库、执行DDL/DML、修改数据集、访问行情网络或控制backend/worker/scheduler；真实文件验证用审计钩子拒绝socket连接、subprocess和任务输出范围外写入。

离线工作不需要用户重启。未来生产启用仍须完整immutable release/control-plane接入及独立授权，用户负责后端重启；不能仅凭此离线policy或CI通过直接运行生产账户。

## 9. DESIGN-COMPLIANCE-001逐项检查

1. 禁止简化交付：接入、原288模拟、12账户真实连续研究分别报告；F-010未完成，不声称全部完成或请求合入。
2. 禁止静默错误：价格/数据/身份缺口显式阻断；未成交不删行，未知不填零；恢复保留已确认前缀。
3. 禁止改变业务逻辑：核心参数、原288人口和费用、旧TWAP/release/账户不改；年初连续研究明确属于用户追加范围和独立闭环口径。
4. 禁止私增审批：遵循既有跨owner边界和完整F2验收，不新增审批、不重复请求已有合入授权；满足全范围条件后可直接合入和本任务清理。
