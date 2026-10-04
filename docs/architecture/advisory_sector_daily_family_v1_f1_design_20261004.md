# Advisory M1显式日频family与D可见分类消费者 F1

2026-10-04；VERIFIED_LOCAL_SOURCE_READY。承接M1日频F2设计；本切片不宣称完整API/UI交付或模型收益确认。

## Background / Goal

用户已重启，运行态HEAD 5c51aac49d7cf951f2835a5e68a04c837f803162；只读preflight对现有QE包返回READY_BASELINE_ONLY、qualification_rechecked=false且无blockers。#5429/#5432/#5434已合入并完成自身清理。真实行情source已交付，但公司分类尚需调用方手工提供，旧九字段daily服务不能消费M1的15D+query16。直接使用冻结M1权重，不恢复QE资格/原native/收益确认门。

## Scope / Non-goals

精确写范围仅：

- backend/services/advisory_model_first/economic_sector_classification_source_v1.py
- backend/services/advisory_model_first/economic_sector_daily_family_v1.py
- backend/tests/advisory_model_first/test_economic_sector_classification_source_v1.py
- backend/tests/advisory_model_first/test_economic_sector_daily_family_v1.py
- 本文
- docs/architecture/advisory_sector_price_daily_consumer_v1_f2_design_20261004.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不改QE、Selection、行业公共resolver、数据准备、交易执行或DB源码。无新fit、研究收益读取、sealed访问、DB写入、profile激活、依赖安装或进程控制。尚未连接原生daily候选DB、HTTP或UI，不复制旧服务或改写旧v3合同。

## Design

分类适配器只调用已存在IndustryPitResolver的公开resolve，以原候选D请求AS_PUBLISHED_PIT/CAUSAL_DAILY_NEXT_TRADE分类，不消费当前行业覆盖历史、不需要候选profile截止日期与D相同。缺resolver/无知识时钟/分类未覆盖保留UNKNOWN；冲突、错误股票/日期/receipt或未来known_from按计算矛盾报错，不伪造行业receipt。未覆盖未来D不能外推旧分类。

显式family为M1_SECTOR_PRICE_VALUE_V1，接收冻结权重reader、原candidate包/program/manifest/policy/universe scope和价格context。原名单不重排、不删股票，不替换matched臂，不使用fitted.request或旧九字段scope。单日调用复用batch同核，最多20日、每日至多20候选；来源一次load_batch，内部已有7个有界只读SELECT。

D特征15维，actual_gap_bps仅作为query第16维。使用原sector_nodes_v1、完整法律tick格点、每股票最多5000点；不稀疏采样、连桥支持洞、改成本或阈值。context必须对应原symbol/D/T且价格单位为已转换CNY；错位输入报错，正常缺context保留QUERY_DOMAIN_UNAVAILABLE，无日限制/超预算明确不可计算。

每价格带报告原条件expected_net_bps范围与path downside_q90_bps最大值；不是盈利概率、统计CI、最佳时点、订单成交承诺或卖出目标。多段/空集合/全部UNKNOWN分别展示。UNKNOWN不使用研究baseline控制制造产品TAKE，也不自动资金仓位。模型原NAVIGATION_ONLY/deployable=false、RECOVERED_LIMITED/native UNPROVEN只披露、不作为消费拒绝。

family原scope检查只防模型与输入拿错；source输出必须与原候选顺序、日期及15字段内容相符，pandas行号不是业务身份。内容hash绑定本次计算，不冒充原生捕获。batch总计算预算30秒，超预算明确错误、不返回伪完整结果。最小真实功能验证使用已消费2024-08-01原20候选，不读T行情/label/新收益；与旧M1价格集合和15D作直接parity。

## Implementation Plan

1. 在独立最新main工作树实现两叶，复用现有source/kernel。
2. 方法、时间/输入、交付/边界三轮本窗口自审，发现问题修复后定向复验。
3. 运行changed-file Ruff、直接矩阵、F1/F2索引、diff/scope核对；通过必需CI后按已有授权合入与自身官方清理。
4. 下一业务切片接原冻结daily候选、显式HTTP/API/UI；不等待原native或收益确认。源码合入与用户重启分账。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-786 | 分类只消费D可见公开resolver，正常UNKNOWN保留，冲突与未来身份拒绝 |
| F-787 | 显式M1/15D+query16、scope及原顺序匹配，无旧九字段/资格审批 |
| F-788 | 单批同核、5000完整tick、多段/支持洞/空与UNKNOWN，不改原数学 |
| F-789 | 原条件收益与path风险语义、功能非收益确认，0fit/0写库/不越界 |
| F-790 | 直接测试、多轮审核及真实原D验证，切片/合入/runtime分别报告 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-786 | economic_sector_classification_source_v1.py | backend/tests/advisory_model_first/test_economic_sector_classification_source_v1.py | VERIFIED | none |
| F-787 | economic_sector_daily_family_v1.py | backend/tests/advisory_model_first/test_economic_sector_daily_family_v1.py | VERIFIED | none |
| F-788 | economic_sector_daily_family_v1.py / sector_nodes_v1 | backend/tests/advisory_model_first/test_economic_sector_daily_family_v1.py；原D价格集合直接parity | VERIFIED | none |
| F-789 | family结果与原scope | backend/tests/advisory_model_first/test_economic_sector_daily_family_v1.py；本文真实验证 | VERIFIED | none |
| F-790 | 本文/两直接测试叶 | artifact: 27定向测试与原20候选真实DB只读验证 | VERIFIED | none |

## 审核修订与真实功能验证

方法轮：核对candidate16维、唯一query缩放、原四头不重fit；修正负净值测试样本（原.98在低价点仍有净值，改为确实全负的合成JSON头，不改生产阈值）。时钟/输入轮：补原symbol/D/分类receipt、未来known_from和非as-known拒绝；修复不应把pandas行号当身份的问题，补畸形上市日期、严格数值/禁止数字字符串及超预算原请求数量。交付轮：核对7文件范围、QE直接使用、NORMAL UNKNOWN不删除、没有HTTP/UI或资金执行的冒称。三轮均为本窗口不同视角自审，不冒称独立外审。

最终27直接测试和changed-file Ruff通过。真实原D验证从冻结原20候选、公开IndustryPitResolver D请求、既存只读DB源到真实M1 JSON头，得到9条ACCEPTABLE_PRICE_SET/11条UNKNOWN_INPUT_OR_SUPPORT；15D按rtol=0/atol=1e-12/equal_nan及全部区间与原M1数学严格一致，完整tick最多1491、行情source 7 SELECT，端到端约8.516秒。原model SHA 872acff3894c7a64b1b87c51ebd440d739a82069d68be0e30ea27dee9c81931e不变。只查询已消费2024-08-01 D及之前行情；原研究report由既有reader读回，不读取新收益/label/T行情/sealed、无fit/DB写入/QE提交/角色启用。

DESIGN-COMPLIANCE-001四项逐项通过：本F1切片完整真实验证但完整daily/API/UI仍待后续；正常缺失与矛盾均可见，无填零/删股/假成功；不改原数学/策略参数/历史结果；无父时钟/native/收益确认等新增审批。最终F1与上级F2索引、diff/scope及必需CI分别验收，merge和用户runtime激活分账。

## Risks / Rollout / Rollback / Production Gates

风险：分类结果错位、D/T未来输入、价格单位、支持洞被连桥以及子切片冒充完整业务。回滚仅停止新family调用，旧v3原样；不回滚数据。合入不启动任何后台任务；后端激活仍归用户。真实原D验证可能只读DB但不写入；不请求数据窗口进行业务验证，不另建证据平台。
