# Advisory 单日/训练同核D特征 F1 Feature Card

## Background / Problem

原每日消费者尚未合入且缺六UI；不能借该阻塞暂停所有研发。纯反例已发现同一market_up_ratio公式在长训练面板与两日每日面板上会因缺行后复牌而不同（.990099/1.0），复用公式不等于复用信息。旧scope已UNPROVEN，不能恢复其证据或扩大旧实验。本切片完整交付独立有界D输入计算API，供未来训练和每日模型消费共同调用，不拟合或改判旧模型，不声称已有完整每日API/UI。

## Scope / Non-goals

精确三文件：backend/services/advisory_model_first/economic_daily_feature_core_v1.py、backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py、本F1 Card。无旧daily source/API/UI/QE/Selection/数据/公共验证/Paper/Execution变更。无DB/网络/文件读取写入、训练、收益读取、研究登记、binding/profile/依赖/进程操作。临时全X。此源码无需重启，不自动改任何producer或已保存工件。

## Approach / Contracts

唯一纯函数使用调用方显式提供的原候选及D可见原始输入；返回完整12个D字段（旧8+新4）与有界内容hash/缺失原因。假设买价query_gap_bps不是D观测，后续查询才加入，不读T价/label/收益。完整原名单≤20、显式candidate_group_size等于原名单长度、原rank连续/唯一、两腿归一值与实际权重、combined_score数值一致；不重新选股或放大到Top40。

日历固定20个≤D交易日加合法下一T，D/T对应候选；交易日历权威性由上游来源身份另验证，纯计算不伪造原生receipt。候选原始OHLC/成交量/成交额/复权因子/涨跌幅报价≤400行；同一原始输入派生D复权视图和raw volume，前三收益/ATR复用既有shared builder和停牌归一化，新四字段复用已合入calculator，不能将复权volume当raw。指数固定20日沪深300，市场宽度只接受最后两个市场交易日、≤20,000行；缺行股票不跨更多历史carry来改变分母。未来训练必须用相同单日包逐D调用；批量只是多个单日包，不在一个长panel上预先算pct_change再切片。

正常缺行情行、nullable字段值、停牌或未知分母保留候选和全部D字段，逐字段UNKNOWN，不补0或删股；必需列缺失属于schema错误，optional报价列缺失投影为未知。真实S/R来源冲突仍fail closed。OHLC有值时必须满足low≤open/close≤high；指数ret_5必须有最后六个连续交易日close，中间缺日也保持UNKNOWN。数据范围、类型、重复键、跨D/T、foreign symbol/benchmark、非法数值或父分数矛盾fail closed。空原名单合法返回NO_CANDIDATES。hash绑定规范化计算输入而非数据库原始字节；实际原生成/采集/数据库版本/PIT身份和收益资格均不由本函数证明；旧训练时序资格不升级。

## Implementation Plan

先两轮设计审核，再实现有界验证/单一raw坐标投影/12字段输出。对新计算函数使用已有fixture和手算反例，验证同一D包不受调用方历史/执行顺序影响；更长面板先明确截为包，函数自身拒绝越界。不得新建独立数据平台或旧实验验收链。多轮源码审核，失败先nodeid，稳定最小矩阵一次，Ruff/diff/F1验证，PR及必需CI。

## Verification Plan / Design Acceptance Index

| ID | 完整条款 |
|---|---|
| F-599 | 完整原候选/两腿分数权重/合法空名单，保序不重选 |
| F-600 | 固定20D/2D信息与D/T/PIT无未来输入，单D与逐D批量同计算 |
| F-601 | 同一raw投影的旧8和新4，收益/ATR/shared停牌及raw volume语义 |
| F-602 | 正常缺失保留、矛盾fail closed、有界输入/hash确定性、不修改输入 |
| F-603 | 独立纯消费API，无旧模型补证/训练/数据库/角色与部署升级 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-599 | backend/services/advisory_model_first/economic_daily_feature_core_v1.py 原名单与两腿验证 | backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py 原rank/score/空名单用例 | PASS | none |
| F-600 | economic_daily_feature_core_v1.py _frame、20D/2D包和D/T校验 | backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py 市场窗口反例、四源未来毒化、同包重复调用；批量调用方逐包复用，不声明批量调度交付 | PASS | none |
| F-601 | economic_daily_feature_core_v1.py 复用shared_feature_builder、suspension_aware_bar_policy、economic_daily_information_v1 | backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py 手算/shared对照、split/raw volume、真实停牌、指数中间缺日 | PASS | none |
| F-602 | economic_daily_feature_core_v1.py 有界schema/OHLC数值校验、逐字段UNKNOWN及规范输入hash | backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py 正常缺失、复权未知、S冲突、OHLC矛盾、重复/预算、Decimal/hash/输入不变性 | PASS | none |
| F-603 | 三文件精确范围；economic_daily_feature_core_v1.py 纯计算无I/O/fit入口 | backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py 明确deployable/outcomes/native=false及old_training_parity=UNPROVEN；源码范围审核 | PASS | none |

稳定矩阵：2026-10-03，`python -m pytest backend/tests/advisory_model_first/test_economic_daily_feature_core_v1.py -q -p no:cacheprovider --basetemp X:/AIstock_temp/advisory/daily-core-final-matrix`，24 passed，2.44s。Ruff通过；本矩阵仅证明计算API合同，不是经济有效性、实盘API/UI或旧模型训练身份的证明。

## Risks / Review

纯计算一致不代表旧训练身份完整或收益有效；当前模型仍未确认。两日breadth是未来统一输入合同，不是改变已消费研究。审核1：剥离未验收旧UI依赖、禁止修改原研究；审核2：统一raw→旧8/新4，避免两个独立source数值相同而身份不同；完整原rank和数值须先验证。源码审核2发现OHLC开盘矛盾未拒绝及指数中间缺日仍计算，先新增失败用例再修复，两项直接节点通过；审核3追加复权正常未知/S来源冲突区分通过。最终矩阵首次仅未来毒化测试追加第21条指数记录使预算门禁先触发，修正为替换原行隔离时间门禁，不放宽断言；四节点及24例稳定矩阵通过。真实源缺口仍由本窗口核验后向数据窗口提确切需求，不转移业务验证责任。

## Production Gates / Rollout / Rollback

DDL/DML/profile/dependency/runtime gates=noop；study trial=0；deployable=false。新纯函数明确可供下一模型/消费者使用，未接默认调度、不替换旧算法；本切片完整但不是整套荐股交付。后续caller显式消费，回退只停止该调用，不覆盖旧模型或数据。
