# Advisory 数据集/股票池统一消费 F1 设计 v1

日期：2026-10-03。归属：Advisory/model_first。范围由用户“统一数据集和股票池，避免类似问题阻断研发”批准。

## Background / 背景

原386D/7720冻结候选不能用当前252交易日股票池重新筛选。日期化行业映射7706行，严格知晓分类3729行，3977行知晓时间未证明，14行在当前池准入之前；不是普遍行情缺失。行业指数成分源只有4股，不应成为普通行业分类的准入门禁。此前以不存在的“行业权威窗口”作为研发前置不合理。

## Scope / 范围

仅新增 `backend/services/advisory_model_first/economic_context_consumer_v1.py`、`economic_context_preparation_v1.py`、对应两个定向测试文件、本设计及主蓝图。独立工作树从`2d9bed020`起步。复用公共只读 `load_qe_profile`、sector pins及IndustryPitResolver；不复制或改变数据准备策略。

## Non-goals / 非目标

不改QE/Selection/HMM/公共行业生成器，不补库/重建数据集/激活profile，不重选历史股票，不消除UNKNOWN，不训练或读取评价收益，不新增生产模型family/API/UI，不改自然采集/confirmation/实盘校验。此次交付是可执行研究准备消费者，不是整个荐股或收益模型完成。

## Contracts / 契约

- 数据身份：调用必须显式传profile文件及其预期SHA256；公共加载器解析同一generation/release/cutoff、controller root及QE六池描述。无旧路径回退。股票池取profile的sidecar hash，`stock_universe`始终与单池/多池并集交集，复用Advisory的QE兼容selection规范化。
- 父身份：显式prepared manifest及预期SHA256；校验stage摘要和四个读取文件hash/size（features/identity/frozen_request/frozen_rankings），核对program/binding/package/request/runtime/policy/cost及原候选rank/hash。先校验原preregistered计划/父链和既存开发窗口授权，封存窗口不能读取。features仅投影三个原键，rankings仅投影原键/rank/候选标志；D/T合法唯一且顺序/人口精确一致，不读取label/price/收益。原生证据等级及原池身份原样保留，与当前release池分开报告；不把当前池升级成原历史池。
- 所有人口：保留输入顺序、所有日期和股票。池外标记 `OUTSIDE_CURRENT_POOL`，探索准备不删除也不重新Selection；仅本明确`EXPLORATORY_SCREEN/NAVIGATION_ONLY`入口允许。正式路径不接此准备入口。普通缺失/停牌不改变成员归属。
- 可选分类：仅严格AS_PUBLISHED_PIT/CAUSAL_DAILY_NEXT_TRADE，category取原生resolver已证明结果；未知category为null。日期化membership单独报告，不作为PIT替代；不调用行业指数成分resolver或假造known_from。未证明知晓时间/分类缺失是逐行UNKNOWN；矛盾/歧义/重叠/异源或hash错误仍硬失败。
- 分层就绪：`core_input_identity=VERIFIED`仅证明研究输入/当前数据身份；`optional_classification=PARTIAL/AVAILABLE/UNAVAILABLE`报告可用数。可选增强缺失不停止基础设计。fit必须另走现有窗口预登记/监督/PIT/QE互斥，UNKNOWN不能喂入模型或当作零/默认分类。比较增强模型与matched baseline只在同一可证明支持内拟合，评价仍保留完整经济人口和UNKNOWN分支。
- 出口：公共pure batch函数返回带原键、当前池资格、日期映射和严格category的DataFrame与摘要；只读prepare串接真实文件，CLI仅stdout输出摘要，不写产物。下一个研究prepare通过这两个公开函数消费，单日和批量使用同一行语义。不新增平台/审批/UI。

## Design Acceptance Index

| design_item | 要求 |
|---|---|
| F-267 | 一次固定profile/release/hash，六池及单池/并集与canonical交集，禁止跨身份/旧路径 |
| F-268 | 原prepared hash链、业务身份/政策核对，原键完整无重选，无收益读取 |
| F-269 | 日期覆盖与知晓覆盖分开；严格category与UNKNOWN，不强加行业指数成分 |
| F-270 | 池外/正常可选缺失不阻断探索准备；矛盾/hash/未来或异源硬失败 |
| F-271 | core身份与可选就绪、原历史证据分层，不升级原生资格，不改变正式路径 |
| F-272 | 单日/批量一致，可执行真实prepare和CLI，不是未使用诊断工具 |
| F-273 | 真实7720输入只读验证、范围测试、多轮自审，无DB/QE/服务操作 |

## Implementation Plan / 实施方案

1. 设计自审：时钟/人口/范围三视角，冻结上述公开接口。
2. 实现profile sidecar读取和pure分类消费；一个run只读取各文件一次，末尾验证profile/sidecar/实际读取源未漂移。不为每行重复IO。
3. 最小prepare验证接入，保留旧输入，不在共享代码提供兜底；定向测试和真实键检查。
4. 审核修复循环，F1验收、PR CI合入及官方自身清理；继续G1设计，不等待虚构窗口。

## Verification Plan / 验证方案

定向测试：单/并集及交集、池外与UNKNOWN保留、单日/批量一致、知晓时钟、异源/冲突/漂移/原身份/hash篡改、仅键投影不读取收益、CLIprepare入口。真实验证只读原prepared及活动profile绑定的source；摘要另存X临时收据，不能将identity PASS当模型/收益PASS。相关完整回归交CI，不运行整个model_first套件。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-267 | economic_context_consumer_v1.py | backend/tests/advisory_model_first/test_economic_context_consumer_v1.py | VERIFIED | - |
| F-268 | economic_context_preparation_v1.py | backend/tests/advisory_model_first/test_economic_context_preparation_v1.py | VERIFIED | - |
| F-269 | economic_context_consumer_v1.py | backend/tests/advisory_model_first/test_economic_context_consumer_v1.py | VERIFIED | - |
| F-270 | economic_context_consumer_v1.py | backend/tests/advisory_model_first/test_economic_context_consumer_v1.py | VERIFIED | - |
| F-271 | 两个消费者、蓝图§16.5 | backend/tests/advisory_model_first/test_economic_context_preparation_v1.py | VERIFIED | - |
| F-272 | prepare_economic_context_v1、main | backend/tests/advisory_model_first/test_economic_context_preparation_v1.py | VERIFIED | - |
| F-273 | 本设计、真实prepare摘要 | validation-receipt: X:/AIstock_temp/advisory/value-anchor-next/context-prepare-summary.json | VERIFIED | - |

## Risks / 风险

将PARTIAL误当模型可用是主要风险，出口明确零fit/非部署、只返回证明category；下一模型不得直接使用未知日期映射。旧原生池不完整仍保留RECOVERED_LIMITED，不能借当前数据统一掩盖。新profile必须显式换身份重新prepare，不在一个run中漂移。共享生成器的通用knowntime缺口不在本次修复范围；现有已证明类别足以开展有界设计，不强制100%覆盖。

## Production Gates / 生产门禁

backend_restart_required=false（新增离线消费者，不改变运行服务）；DDL/DML、依赖安装、数据激活均not_required。正式自然采集/confirmation原合同不改，零activation。后续真正服务接入仍由用户重启，另做运行态验收。

## 实际验证与重复审核

真实prepare约6.17秒，386D/7720/938原键不变；core身份VERIFIED、行业PARTIAL（3729已证明/3977知晓未知/14当前池外）。原RECOVERED_LIMITED未升级，零收益读取/fit/DB/进程操作。profile与读取文件完成末尾漂移校验。入口也支持不请求行业的core-only准备。

三轮同窗口自审（不冒称独立外审）：①工程轮修复Windows CRLF原始字节hash误判，并绑定profile coverage收据；②方法轮补齐原计划/窗口授权、原dataset manifest/request/rank复制hash和完整人口顺序，不读取收益；③安全/一致性轮区分core身份与特征/模型就绪、可选UNKNOWN与硬冲突，并将蓝图页首/§1.4/§6.3.3/阶段表/价格候选验收条目/§16活动队列同步，历史实验不改判。测试fixture错误（冻结dataclass注入、四类窗口、规范收据文件名）已修复，不修改业务来迁就测试。

DESIGN-COMPLIANCE-001四项：完整交付上述7项消费者范围，未声称模型完成；无填值/回投/虚假成功；无QE/公共数据/执行模块变更；无新增外部窗口、UI审批或100%分类门禁。
