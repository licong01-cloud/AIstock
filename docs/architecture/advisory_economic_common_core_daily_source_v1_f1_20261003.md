# Advisory 同核日频只读输入适配 F1 Card

## Background / Problem

#5313已交付纯D计算内核，下一步需要从数据库给单日/历史批量提供完全相同的有界输入；不为旧失败模型重新回放或补身份。本切片只交付完整独立读取API，不依赖尚未合入的每日UI/API，不报告收益或正式可买。

## Scope / Non-goals

精确三文件：backend/services/advisory_model_first/economic_common_core_daily_source_v1.py、backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py、本Card。无QE/Selection/数据准备/公共验证/Paper/Execution变更，无训练/收益/sealed访问、DB写/激活/依赖/服务操作。X临时，F源码。每日候选由已验证上游显式提供，source不重选股或生成native。设计与源码分别验收。

## Approach / Contracts

load_day委托load_batch的同一路径，每块1..20个单D输入；每包含原candidates、21日calendar、实际component_roles/terminal_weights。输入预检时间/名单规模/symbol，完整数值最终由唯一core验证。无日期硬编码，不替换QE全局窗口/股票池。

一块同一REPEATABLE READ readonly事务，finally rollback。最多5个SELECT：完整每包日历start..T、候选20日精确(date,symbol)键集raw OHLC/amount/volume+adj、20日CSI300并集、最后两日PIT合格SHSZ市场宽度并集、精确候选S/R/timing；全块空原名单只查日历，不读行情。无逐股/逐日SQL；raw最多8000键、S/R最多16000事件、calendar最多420行、benchmark最多400、market最多400000。各SELECT LIMIT=预算+1，检查全局范围/重复后逐包切片，不能过滤外部行掩盖冲突；每包仍服从core400/800/20/20000预算。调用方必须使用已批准窗口，不读未消费窗口或未来T行情；下一T仅验证日历。

每SELECT用总30秒剩余预算设statement_timeout≤15秒，不重复行情校验或额外查询；SET LOCAL仅事务设置，不是业务写入。输入快照只证明当前只读值，source_evidence仍COMPUTATION_ONLY、native=false、old_training_parity=UNPROVEN。规范输入/feature hash在同源单/批时一致，实际read_at/query timing单列，不要求两次真实快照时钟相同。

复权/缺失/停复牌/两腿/原名单完整性全部调用core，不再实现公式。正常缺行保留UNKNOWN，超时/超限/重复/外部股票或日期/schema失真fail closed，不以空名单冒充缺输入。reader不发布capsule/角色，正式来源身份由上层原生合同另验证。

## Implementation Plan

两轮设计审核后实现SQL边界、事务、单/批共路与同核计算；复用已有core fixture，fake连接只验证source合同。失败先nodeid，稳定最小矩阵一次，Ruff/diff/F1与CI；真实业务只读核验如执行只选已消费最小D、不读取收益、不重建旧产物。源码合入无需重启，不自动接调度。

## Verification Plan / Design Acceptance Index

| ID | 合同 |
|---|---|
| F-611 | 原输入与D/T预检，有界5SELECT、精确候选键和PIT市场范围 |
| F-612 | 单/批同核及相同输入hash、候选正常缺失保留 |
| F-613 | readonly单快照、预算/异常rollback，拒绝外部/重复/schema数据 |
| F-614 | 无训练/收益/QE/数据激活，source/native/经济资格不升级 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-611 | backend/services/advisory_model_first/economic_common_core_daily_source_v1.py _packets/read/精确SQL | backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py 请求预检与SQL预算、完整日历 | PASS | none |
| F-612 | source.load_day→load_batch→build_economic_daily_feature_core_v1 | backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py 不同D单/批同值hash、缺候选行情/合法空名单 | PASS | none |
| F-613 | source单快照、动态statement_timeout及finally rollback | backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py 外部/未来/重复/schema/超限、SQL/超时/日历/回滚失败 | PASS | none |
| F-614 | source三文件范围、db_source证据与core资格不变 | backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py readonly/no-DML/非native非deploy；下方真实SQL smoke | PASS | none |

最终稳定矩阵：`python -m pytest backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py -q -p no:cacheprovider --basetemp X:/AIstock_temp/advisory/common-source-final`，14 passed/1.41s；Ruff通过。测试复用已有core packet，不复制整套行情fixture或旧研究。

2026-10-03真实只读SQL smoke：以已消费D=2024-07-04、数据库权威T=2024-07-05和两个**合成候选分数投影**验证新数据库读取接口，不是原Selection名单或荐股研究。source实际5个SELECT：calendar21行/0.016s、raw40/0.750s、benchmark20/0.031s、market10169/2.312s、suspend0/0.016s；两行12D值无UNKNOWN。时钟准备另2个只读SELECT，总7个、所有事务rollback。outcomes/native/deployable/database_written=false；仅REAL_READONLY_SQL_SMOKE_SYNTHETIC_REQUEST_NOT_RECOMMENDATION，不当成PIT/native、模型利润、全量批处理性能或原名单业务验收。

## Risks / Review

设计审核1：固定精确(date,symbol)请求，不读跨候选/日期交叉组合；日历不是只查声明日期集合。审核2：市场宽度沿用既有PIT eligible SHSZ定义，不误用当前指数池；同源值一致不升级native，也不改变旧模型。源码审核1/2核对全局数据先验、单/批同核和预算，移除空名单不必要行情查询；审核3发现rollback失败会泄露未封装异常，先失败再修复并通过直接节点，稳定14例通过。真实SQL仅闭合新查询兼容性，不借合成候选声称原名单、经济或native验收。

## Production Gates / Rollout / Rollback

DDL/DML/profile/dependency/runtime=noop，study=0、outcomes/sealed=0、deployable=false。独立显式读取能力完整交付，不代表完整日频API/UI或可盈利模型；后续caller按真实scope调用，回退仅停止新调用。用户服务/QE任务不变。
