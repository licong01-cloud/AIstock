# H-TIMING-1 两个历史描述量及只读同源入口 F1

## Background / Scope

已批准父设计：advisory_economic_entry_timing_hypothesis_v1_f2_design_20261003.md §4–6、8步骤2。本切片只完整交付纯计算和原数据读取复用，不交付训练、收益结论、价格区间或角色激活。

精确允许五文件：
- backend/services/advisory_model_first/economic_entry_timing_features_v1.py
- backend/tests/advisory_model_first/test_economic_entry_timing_features_v1.py
- backend/services/advisory_model_first/economic_common_core_daily_source_v1.py
- backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py
- 本文。

## Approach / Contracts

纯函数输入原候选、21日时钟（20D+下一T）、raw daily、S/R；复用现有core结构与数值校验。固定19配对，D adj anchor，contrast=mean(log(O*/prevC*)-log(C*/O*))，vol=std(log(O*/prevC*),ddof=1)。不压缩日历、不填缺失、不消费T行情。每个特征独立按所需原值传播UNKNOWN；缺D close不影响完整的vol，缺更早close影响两个量。原值非有限、非正价格、重复/外部/未来键及OHLC矛盾fail closed。

真实bar要求原正成交量、非synthetic；整日停牌/零量/未知量/不一致S-R/覆盖开收盘或无法解析的停牌时段UNKNOWN。只有明确位于开收盘之间的部分停牌且真实成交端点可证明才允许计算。候选与rank完整保留。输出计算hash及字段原因，COMPUTATION_ONLY、deployable=false，不产生native或训练资格。

source.load_timing_day委托load_timing_batch，内部共用原load_batch实现、同一快照/原值，旧load_day/load_batch原行为与返回维数不变。timing入口返回KEY+12D+2timing及合并receipt，不额外SQL、不公开裸packet、不使用摘要反推原值。仍1..20包、最多5SELECT、30s总预算、单SQL15s、finally rollback。

## Implementation / Verification

设计自审1：固定所需原值及独立UNKNOWN，缺D close只影响contrast。设计自审2：部分停牌端点证明与S/R歧义明确；只读共路不改原消费者。纯计算手算及公司行动、时钟/重复/synthetic、缺失/正常停牌测试；source复用同fixture测试单批同值hash、5查询及默认兼容，失败先定点、稳定最小矩阵一次。不同视角循环审核修复直至无已知问题。

## Design Acceptance Index

| ID | 要求 |
|---|---|
| F-621 | 固定19配对、D anchor、独立缺失及真实bar语义 |
| F-622 | PIT/原候选唯一性/数值/键矛盾fail closed，不删除股票 |
| F-623 | 单/批timing共路、复用五查询、默认12D兼容及回滚预算 |
| F-624 | 计算/源身份不冒充native/训练/业务收益，严格模块边界 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-621 | backend/services/advisory_model_first/economic_entry_timing_features_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_features_v1.py 手算/ddof/拆分/独立缺失/停复牌 | PASS | none |
| F-622 | backend/services/advisory_model_first/economic_entry_timing_features_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_features_v1.py D后/重复/rank/非法factor/收益字段拒绝 | PASS | none |
| F-623 | backend/services/advisory_model_first/economic_common_core_daily_source_v1.py | backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py 新单批同值与旧默认一致、5SQL、回滚、缺D close | PASS | none |
| F-624 | 精确五文件及COMPUTATION_ONLY receipt | backend/tests/advisory_model_first/test_economic_entry_timing_features_v1.py 与source叶测试；scope/diff检查 | PASS | none |

最终稳定最小矩阵37 passed（4.70s终端墙钟）；四个Python文件Ruff通过、diff check通过。未执行全模块重复回归，交给CI。审核1逐公式和信息时钟核对：19配对、D anchor、缺D close只影响contrast，增加source层该边界定点验证后通过。审核2缺失/真实bar/身份核对：零量、synthetic、整日和S/R歧义不伪造正常样本，股票完整保留；明确部分停牌端点限制。审核3默认兼容及范围：新入口仍原5SQL/事务，原12D输出及hash不变，新14D receipt嵌套保留原核证据；修正Ruff E731后复验。均为本窗口不同视角自审，不宣称独立审核人。

DESIGN-COMPLIANCE-001：完整交付本F1计算/来源能力，后续研究未完成；UNKNOWN和矛盾显式，无默认填值或静默失败；候选、日历、公式与旧接口合同不变；未新增审批/数据准备/未来交易日门禁。设计与源码验收不等于真实原候选覆盖、训练或收益通过。

## Production Gates / Risks / Rollback

只改Advisory五文件；无QE、Selection、策略包、数据准备、Execution变更。无真实DB写、数据激活、依赖安装、训练、收益读取、sealed、服务控制。X临时、F持久。停止新显式入口即可回退；原消费者不切换。历史DB身份不自动成为原生PIT。源码模块未被现运行态引用，用户后续加载与模型资格另报。
