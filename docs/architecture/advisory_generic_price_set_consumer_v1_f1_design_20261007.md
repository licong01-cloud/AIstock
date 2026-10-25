# Advisory 固定五交易日通用价格集合消费者 F1详细设计

2026-10-07，SOURCE_VERIFIED。本切片是必要模型输出适配，不新增模型、平台或UI；源码提交/当前CI状态另报。

## Background / Goal

GP5、GP5-MINUTE、GP5-VOLUME-PATH与GP5-JOINT-DISTRIBUTION已有独立训练和价格数学；目前缺少让调用方按既存权重、原候选D输入及合法价格上下文获得一致业务输出的轻量消费者。旧M1日频接口有五次有效review标签，不能静默换成固定五交易日，更不能借M1未配置阻止新模型研究。

本功能只完成统一只读权重装载与纯价集投影：允许不同策略包/stock_universe/单指数/指数并集提交同schema已计算D字段，不读父分数、腿、名次或包ID作为预测输入；输出建议的可能盈利买价集合、净收益/风险口径和证据状态。它不是开盘预测、最佳分钟点、卖价模型、订单或收益承诺。固定T至T+4，不把五review等同五session。

源/研究/经济/日频数据接入分开：该完整F1切片不增加API、DB读取、调度或发布；现有每日DB读取不变。调用方可以提供来自DB的D输入，但不能因此宣称全部四类模型已接入实盘API。EOD九字段DB适配及D分钟可选来源后续单独精确切片，不能用中位数手工填充冒充已测量分钟信息；本切片不会阻止已prepare的联合分布一次研究。

## Scope / Non-goals

设计阶段仅本文件和蓝图；源码阶段最新main独立四文件：

- backend/services/advisory_model_first/generic_price_set_consumer_v1.py
- backend/tests/advisory_model_first/test_generic_price_set_consumer_v1.py
- 本文件
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

只读现有四个模型类型、纯query/价集函数及stage/JSON帮助函数，禁止修改旧模型/训练pipeline/权重/输入/label/policy。禁止QE、Selection、HMM、StrategyPackage、行业、公共数据、Execution、Paper、CI、workflow、AGENTS写入。0fit/收益结算/旧收益重读/DB/安装/配置激活/服务或进程控制；临时测试X，原正式模型F不改。只消费一个显式模型引用，不能自动按历史收益选赢家。

## Architecture / Contracts

### 1. 一次显式装载，不重新审核策略包

load_generic_price_set_model_v1(model_family, trained_manifest_ref)接受四种固定family：DAILY_5TD、MINUTE_5TD、VOLUME_PATH_5TD、JOINT_DISTRIBUTION_5TD。引用是原trained/manifest.json绝对非C、非重定向路径、SHA256与size；不得接受任意pickle、执行代码、URL、活动profile或包资格审查。只读取trained manifest及其唯一model.json，manifest文件限制1MiB、模型64MiB，先size/路径再读取；读取后核对文件SHA/size及stage内容hash、model.json描述符/模型身份和各family已有validate_fit，不要求当前源码字节SHA等于旧producer（避免Git CRLF差异成为权重消费门）。

manifest必须是trained而不是prepared/evaluated；stage仍绑定原plan/parent/model文件，必须返回producer plan/stage标识。缺训练文件返回明确MODEL_NOT_TRAINED，不自行fit、等待自然日期、补数据或换模型。损坏/身份矛盾抛AdvisoryModelFirstError；探索性、负向或非vintage来源不是装载阻断，证据状态如实展示，不能把可读权重称为已通过收益。没有收益门/MDE门/native资格门，没有隐式fallback。

只读产物数据身份；内存对象deep-copy并不暴露可变内部模型。对同一次load→project的调用，引用/输入固定，重复消费不训练；最多一个模型/最多50原候选，无全局新缓存服务。

### 2. 完整D输入与价格上下文

project_generic_price_sets_v1接受loaded、candidate_rows、price_contexts、source_context。候选字段是原KEY(D/T/instrument)、selection_effective_rank、candidate_group_size及所选family的原特征schema（9/17/19量）。最多50、0只合法；全部D/T一致、D<T，股票与rank唯一、排序原样、group_size为原完整流大小且>=当前人口/rank（不强行改为截断Top20大小）。rank/package/pool只是元数据不进数学。调用方明确声明feature_visible_through<=D；不存在读取T行情或收益的方法，不查询新股票池/成员/Selection，保留原候选，不补Top6/不重新排序。

price_contexts按instrument提供reference_cny、legal_low_cny、legal_high_cny、tick_cny、visible_through和price_basis=D_ANCHORED_CNY；reference是D锚而非T开盘/当前报价。来源已知矛盾/未来字段拒绝；普通缺上下文或已知D close缺失返回逐股UNKNOWN_PRICE_CONTEXT，不把正常停牌删除或令整批失败。已有模型决定特征缺失的UNKNOWN/median+flag策略；调用方显式null/NaN保留，不手工填中位数或零、不压缩session。只声明消费时钟，不伪造historical vintage/native capture。

source_context只含package_id/run_id/list_version_id/universe_identity/source_evidence/feature_visible_through；包/池身份可显式null，有限JSON且有界。真实提供的标识非空、未知证据不升级；数学在相同字段与价格坐标下跨包/池/原rank一致。字段及上下文范围由正常数据矛盾校验约束，不新增策略包准入。

### 3. 统一业务价集和界限

只调用family原candidate价集函数，禁止matched、规则或多个旧模型自动择优。沿用固定成本buy0.95bps/sell5.95bps、risk800bps、expected_net>0、完整legal tick/多区间/UNKNOWN洞；不结果后改阈值或放宽支持。不消费T实际开盘，也不统计开盘覆盖率。

每个原股票返回metadata、family/model/policy标识、holding_sessions=5、label_contract、price_basis、intervals_cny、状态与逐字段未知。source_context与candidate输入不变；空批返回NO_CANDIDATES、0模型query。UNKNOWN与NO_ACCEPTABLE_PRICE是不同语义：前者信息不足，后者模型当前有信息但没有符合原净价值条件的价格；允许当日没有可买建议，不声称全UNKNOWN等于确定空仓策略。

既存权重的正区间仍只是模型估计，不证明成交、因果可达或真实超额。统一输出evidence_use=NAVIGATION_ONLY、deployable=false、economic_confirmation=false、decision_clock=D、hypothetical_price_not_order=true；不输出目标资金、卖点、执行算法、经济PASS、用未知空槽伪增量或复利NAV。新联合模型可能尚未trained，loader不得阻断其它已训练family的独立使用。

### 4. 性能/副作用

一次只读模型文件，逐股调用已有有界价集；每股原函数最大100000 legal ticks，调用方deadline预算30秒，开始/每股前后检查，不丢弃超预算股票伪完整成功。超预算返回计算错误且不发布半批。先验证完整候选/上下文时钟，再数值query，未来/非法行不会被先算后丢掉；不每天重建工作树或模型。无新磁盘cache、DB连接、QE API、网络、训练、Selection、环境设置或任何激活/输出文件写入。

## Implementation Plan

设计三视角修订/F1/diff通过后合入→最新main四文件源码树→固定JSON装载和family dispatch→纯逐股投影→PIT/UNKNOWN/跨包、产物身份和副作用多轮审核修复→一套小模型fixture稳定直接测试/Ruff/L0/F1/当前CI→按已有授权合入、自身官方清理。新的联合研究仍由独立原producer执行且QE串行，不能为这个消费者重fit模型或启动UI验证。

完成只报告本F1模型输出适配，不把它视为四模型实盘接入/盈利完成；必要DB/API下一切片由业务输入规格确定，沿用原不写DB/用户重启边界。模型效果不同于代码能用，既存负候选不为消费者补证。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-761 | 四family显式权重装载、trained产物路径/descriptor/模型身份、缺训练无fit或fallback |
| F-762 | 原0～50候选/KEY/order/rank/group与未知上下文全部保留、输入不变 |
| F-763 | feature_visible_through<=D及D<T、future/重复/价格坐标矛盾拒绝，不读T或Y |
| F-764 | 调用原candidate价集/固定5TD/成本一次/多段与未知洞，跨包池只元数据 |
| F-765 | UNKNOWN与已知空集合不同、证据不升级、不自动选择/激活/形成资金或执行 |
| F-766 | 有界读/预算/四文件范围、0fit/DB/网络/UI/旧源修改、多轮审核和真实进度 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-761 | generic_price_set_consumer_v1.py / loader | artifact: docs/architecture/advisory_generic_price_set_consumer_v1_f1_design_20261007.md §1/Review；backend/tests/advisory_model_first/test_generic_price_set_consumer_v1.py | SOURCE_VERIFIED | none |
| F-762 | generic_price_set_consumer_v1.py / projection | artifact: docs/architecture/advisory_generic_price_set_consumer_v1_f1_design_20261007.md §2/Review；backend/tests/advisory_model_first/test_generic_price_set_consumer_v1.py | SOURCE_VERIFIED | none |
| F-763 | generic_price_set_consumer_v1.py / D clock | artifact: docs/architecture/advisory_generic_price_set_consumer_v1_f1_design_20261007.md §2/Review；backend/tests/advisory_model_first/test_generic_price_set_consumer_v1.py | SOURCE_VERIFIED | none |
| F-764 | generic_price_set_consumer_v1.py / family dispatch | artifact: docs/architecture/advisory_generic_price_set_consumer_v1_f1_design_20261007.md §3/Review；backend/tests/advisory_model_first/test_generic_price_set_consumer_v1.py | SOURCE_VERIFIED | none |
| F-765 | generic_price_set_consumer_v1.py / output | artifact: docs/architecture/advisory_generic_price_set_consumer_v1_f1_design_20261007.md §3/Review；backend/tests/advisory_model_first/test_generic_price_set_consumer_v1.py | SOURCE_VERIFIED | none |
| F-766 | 本scope及副作用约定 | artifact: docs/architecture/advisory_generic_price_set_consumer_v1_f1_design_20261007.md §Scope/Review；git diff --check | SOURCE_VERIFIED | none |

最小测试复用一套可手算JSON小树，不重新跑19/8旧研究矩阵：四family装载和原函数精确价集parity、缺训练与篡改描述符、未来时钟/重复/截断人口保持、未知上下文/空集合/未知洞/空名单、变包/单池/并集时数学相同、不可变及deadline不返回半批。模型fixtures不是真实盈利或新研究trial；对潜在真实artifact只读装载不读evaluated结果。

矩阵当前验收完整四文件纯模型输出切片；不以本切片冒充后续DB/API接入或经济确认。现有三个真实trained权重已只读装载，第四joint仅合成fixture验证接口、真实研究仍prepared-only等QE，不能声称已读joint真实权重。

## Risks / Rollout / Rollback / Production Gates

单父包/已消费开发窗/非vintage及旧negative限制继续保留；统一模型数学不能证明跨包泛化。JSON合法不等于经济有效，引用损坏的真计算错误与策略包资格门分开。未来DB源须提供与训练相同字段/单位，尤其D复权与分钟相对量，不能用本函数名自动声称parity。

完整纯模型消费者不需要重启、DDL、依赖、数据release或模型激活。无默认环境配置/调度/角色pointer；回滚只停止新的纯函数调用，旧API/源/模型不动。后续如增加API加载则用户重启。本设计满足DESIGN-COMPLIANCE-001：全部六项落实才报告本切片source完成，业务实盘/收益仍分报，未授权简化不冒充全产品。

## Review / 多轮自审

第一轮业务：限定为模型输出适配，避免新增UI、旧失败验证、动态仓位或API平台；现有五review接口不变，price-only不研发执行。第二轮PIT：完整候选和逐字段UNKNOWN、真实D锚和可见时钟、不按当前股票池删冻结候选；旧模型加载不用producer源码CRLF差异设置门，stage数据完整性仍校验。第三轮工程：四文件/最多50、预算和只读明确，缺joint训练不fit、不阻其它family；单个显式candidate，不回选旧matched/不读收益选模型，零研究计数。以上仅本窗口不同视角自审，不冒称独立外审或实现完成。

源码第一轮：四family同一手工小树，无训练；JSON支持区间读回list与原immutable tuple合同不兼容，修在本消费者的读回转换，不动旧模型/公共合同；7失败节点定向复测通过，联合fixture此前已通过。第二轮：成本/全tick/UNKNOWN洞与已知空集合、保留原候选/元数据、cross-family parity及deadline不发布半批；新增洞节点通过。第三轮：发现公共canonical JSON hash保留legacy非有限语义，不能依赖其拒绝NaN；本消费者显式finite JSON/重复key校验，未改共享hash，定向节点通过。最终一套10直接测试/Ruff通过，四文件L0/F1/current CI按源码交付流程核对。

真实只读消费者装载仅三个trained JSON：DAILY_5TD、MINUTE_5TD、VOLUME_PATH_5TD各原model SHA与manifest不变。没有读evaluated收益、重fit、注册新研究或生成旧失败确认；这只证明权重消费兼容。新joint根尚无trained时保持MODEL_NOT_TRAINED，不伪造权重；合入#5635不等于研究完成。模型/API/运行/经济状态独立，UI后置。
