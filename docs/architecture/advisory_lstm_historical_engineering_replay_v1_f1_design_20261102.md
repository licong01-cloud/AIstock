# LSTM历史工程回放（F1）

2026-11-02。用户要求程序开发不依赖交易日数据更新，并授权按历史数据验证建议执行。交付单Advisory模块的离线文件回放能力；现有页面仅增加历史证据标识与隔离实时行情。正式前向协议不成为工程门禁。

## Background / Goal / Scope

已有历史prepared inputs15250行、三折预测2000/2150/2150行和finance6300行。三个模型cutoff为2025-03-31、05-30、07-31；全126个heldout D全部进入工程回放，不选最好折/日期。以原Top50、每折原模型、Top10、固定5TD/原费用与UNKNOWN/CASH验证当前计算和展示，不进行新研究政策或经济检验。

显式13文件范围位于X:/AIstock_temp/ADVISORY/lstm-historical-engineering-replay-20261102/scope.json。原模型、预测、finance、历史报告、V1/V2协议和V2十文件源码闭包只读；新的回放根不得落入任何原研究、生产模型或源码目录。数据库、TDX、QE和原生发布均不调用。

## Non-goals / 非目标

0fit/新候选/新原始行情标签/生产写入/服务控制。不是重新估计原模型收益，不构造2025年的生产Program存在证明、历史首次公开时间或独立OOS。底层5TD价格与执行事实已由原标签资产固定，本轮从该冻结合同重新投影估值并验证费用/汇总，不捏造原始价格轨迹或重建成交。运行产物不得写入正式V2研究或替换生产预测。

## Architecture / Allowed APIs

离线适配器读取原P19恢复版本及其引用的既存完整finance源，先核对角色/路径/物理hash和原stage manifest。复用`lstm_specialist_spo_v1.predict_specialist`校验折、训练截止与原heldout窗；重算预测必须与原保存KEY、排名、分数、缺失建议一致。复用`finance_projection`按原折末截止投影已有单股5TD估值；不解锁原IMMATURE行。再调用`lstm_spo_shadow_v1.paired_result`、`lstm_top10_business_review_v1.review_day/summarize`及现有不可变发布器。API名称来自当前直接符号，不另写简化排序器、均值或页面。

模拟时钟显式标注SIMULATED：D收盘后可捕获，T09:30前必须完成，H收盘前只读label_information_end等元数据并返回未成熟，原折截止仍限制可见评价。生产`predict_current/capture`的起始日和真实前瞻限制保留，不改日期骗过它们。历史适配器的存储与源版本注入只用于工程回归，并以原日期和历史模型引用留痕。

所有126D/6300行保留。先比较每D与原候选/原生结果，再对原三臂共同102D复核旧报告；两个完整配对口径若不同须分别披露，不借改变分母生成新收益主张。逐股已扣费净值、现金槽0、未知null及四项分解由公共函数重建并独立算术校验。

离线输出当前Shadow形状的页面数据包，附`replay_context`标记历史工程、NON_VINTAGE、折/训练截止、模拟时钟和来源hash。沿用当前ModelObservation/公共API类型；在现有业务指标上方显示历史标识，将“捕获于”标为“回放时钟”，历史模式不请求实时行情。数据包只是隔离验证传输，不新增生产HTTP路由或后端定时任务。浏览器场景消费实际历史回放生成的固定样本，不能只以合成成熟样本验收。

## Implementation / Verification Plan

1. 新增离线适配器/命令和直接合同测试；禁止fit/DB/实时源，所有来源前后hash一致。
2. 当前公共函数执行三折126D全量历史回放，对账原6300预测及finance、原panel/daily；旧102共同D均值只作算术一致性，不是新经济确认。
3. 历史实数与显式故障注入覆盖21点后/午夜/截止、版本漂移、缺行/重复/NaN、父hash冲突和重用；注入样本不计真实表现。
4. 当前页面验证真实成熟、真实UNKNOWN/未成熟、双Top10/全50筛选、分解、历史标识及不请求实时行情。复用预构建runner-owned隔离frontend，不操作用户backend。
5. 三轮自审（人口/时钟/算术、故障/无副作用、UI/交付边界）及必要修订后最小测试/lint/compile/L0/ownership/CI路由/F1验收；既有授权下提交合入与主目录同步。

## Design Acceptance Index

| ID | Requirement |
| --- | --- |
| F-701 | 绑定原历史来源、三折模型/截止及126D全人口，不新增拟合或选择窗口 |
| F-702 | 当前公共推理、finance投影、十槽配对及业务分解重放，与旧工件对账 |
| F-703 | 显式模拟时钟、成熟前不读金融payload、原IMMATURE/UNKNOWN/CASH保留 |
| F-704 | 晚到/跨日/截止、版本漂移、缺失/重复/非有限及幂等故障验证 |
| F-705 | 当前API形状与现有UI消费真实历史结果，明确历史标识且不混用实时行情 |
| F-706 | 隔离不可变输出、重复调用重用，原工件/V2源码闭包前后不变 |
| F-707 | 独立算术读回、三轮自审、真实历史浏览器及逐项完整交付 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-701 | 离线适配器与CLI | 来源manifest和126D历史运行 | PLANNED | 实现进行中 |
| F-702 | 原predict_specialist/finance_projection/paired_result/review_day | 全历史逐D/逐股对账 | PLANNED | 实现进行中 |
| F-703 | 历史时间适配 | before-H payload guard及原状态计数 | PLANNED | 实现进行中 |
| F-704 | 历史故障场景 | 直接回归及真实数据注入回执 | PLANNED | 实现进行中 |
| F-705 | ModelObservation/API类型/历史fixture | 当前页面真实历史浏览器 | PLANNED | 实现进行中 |
| F-706 | 原不可变发布接口 | hash/重用/拒绝覆盖读回 | PLANNED | 实现进行中 |
| F-707 | 本设计、结果报告和验证回执 | F1门禁/三轮审核/CI | PLANNED | 实现进行中 |

## Risks / Rollout / Production Gates

已消费开发历史只证明工程正确性与旧结果可复现，不证明新独立收益或历史PIT。缺失输入不能补零/前填，旧截止后的评价不得回填为旧已知。原数据损坏或身份不符显式失败，不继续写成功回执。每D最多50行，原126D上界，不运行训练器或读取新市场源。

新增前端历史标识为可选字段，生产缺字段保持既有显示。无后端运行源码改变、生产DDL/DML/依赖/客户端/数据集激活要求；前端加载与源合入状态分别报告。回退仅移除新离线能力和可选标识，不删除旧研究工件；后端重启归用户，不能为本回放启动用户服务。
