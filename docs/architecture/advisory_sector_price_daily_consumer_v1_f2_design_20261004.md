# Advisory M1板块条件价格模型：日频消费者 F2 详细设计

2026-10-04；DESIGN_ONLY_SOURCE_PREPARATION，非每日业务交付、独立确认或正式启用。依赖[确认框架](advisory_sector_price_value_confirmation_v1_f2_design_20261004.md)与[共享D消费者](advisory_economic_common_core_daily_consumer_f2_design_20261003.md)。不以新增接入工程代替收益确认。

## 1. Background / 当前事实与业务目的

R2-M1研究源码#5414已合入且自身清理；冻结开发导航通过，但两个配对增量区间跨零。candidate净收益29.7444%包含36个真TAKE及57个UNKNOWN基线研究控制，不能全部归因模型。原7720键、3505板块可用/3991分类或映射未知/224warmup及RECOVERED_LIMITED、native UNPROVEN保持不变。

冻结模型reader PR #5421已在必需CI run37157479273全绿后合入9cd4e831e18909f62e5d23bd9576d8696b045988；多轮自审、16直接测试及真实四头合成parity通过。只证明冻结研究权重消费，不证明真实每日来源或经济资格。该reader不重新加载训练行情/profile或fit，scope中父模型information_end仍UNPROVEN。两腿权重确实存在，但现存manifest、公开loop config及既有editable config均没有原训练时钟（data_split为空/null），不推导需要重训，也不以当前QE默认split补历史训练时钟。

现有每日v3入口直接读取fitted.request.source_request并绑定EconomicModelScopeV2/九字段及旧qualified类型；M1的SectorPriceFitV1没有request，candidate输入16维，不能按“都是GBDT”强塞旧接口。目标是同一冻结价格条件数学消费严格D可见的新数据，给出支持内可能有净价值的多段/空/未知买入价格集合；不是预测开盘价、最佳分钟或保证成交。

本次仅交付完整详细设计。正NAV允许最小消费者准备，不授权读取sealed/新holdout，不解除原训练身份限制；消费者源码可用不等于此M1可正式荐股。

## 2. Scope / Non-goals / 显式范围及依赖

本设计PR仅本文及蓝图当前路线链接。自己的独立worktree为advisory-sector-daily-consumer-20261004，分支docs/advisory-sector-daily-consumer-20261004，从最新origin/main创建。

后续只实施有独立业务价值的三个小切片，逐切片事前登记而非一次大范围业务PR：

1. 纯D组合与研究查询：新增`backend/services/advisory_model_first/economic_sector_daily_core_v1.py`、对应同叶测试及F1 Card；复用已有common D/sector/math/reader，不改原冻结数学或fit hash。
2. 真实来源适配：必要时新增`economic_sector_daily_source_v1.py`、对应同叶测试及F1 Card；同叶下已有只读common source复用不改，不调用Selection或父包推理生产者。板块源公开合同未明确时只暂停该适配，不建新数据集。
3. 资格成立后才登记现有Advisory服务/router/API类型/价格卡片的精确文件范围；复用既有展示，不先为未确认且原生训练身份未证明的模型开发自动角色或新UI。

上述2/3尚未授权为“已具备来源/资格”。不修改QE、Selection、StrategyPackage、HMM、公共数据、Paper/Execution、公共CI或任何服务；无新训练、候选重建、DB写入/DDL、profile/数据/模型激活或依赖安装，后端重启仍用户执行。消费者不会修复父包生产者缺陷。

## 3. Architecture / 明确family，不复制平台

原run/list候选及D/T/包/政策/池身份→只读有界原始输入→`build_economic_daily_feature_core_v1`的12个D值→`sector_dynamic_rows_v1`的3个D值→独立M1 scope/15D receipt→法律价格格点query→原`sector_price_set_v1`/`sector_nodes_v1`→研究价格集合。qualified生产消费是另一资格路径，当前未存在。

12+3=15个D值，最后第16维是价格查询的actual_gap_bps坐标，不是T的实际开盘或未来行情；matched模型13维只作冻结对照，不回选作荐股。原math把gap bps除100后预测，消费者不可重复缩放或替换成百分比字段。query_gap_bps是展示语义名时必须显式映射actual_gap_bps，不修改原模型schema。

M1 scope显式绑定family、15D字段有序名单、原recipe/数学逻辑hash、原model与reader bundle、package/program/manifest、parent policy/value policy/cost、universe selection、feature语义及来源限制。不捏造旧EconomicModelScopeV2或fitted.request，不用duck-typing绕过旧资格检查。相同维数/可加载权重不能证明跨包/跨池适用；换包、腿、pool或policy必须核对声明范围，否则SCOPE_UNAVAILABLE。

原训练D块由共享core的已冻结recipe生成，板块块原纯函数计算；实施需核对真实训练recipe声明与共享逻辑hash，并用已消费原输入证明数值兼容，不能只对新输入追加语义sidecar即宣布旧训练身份完整。纯组合测试/合成query只能证明算法组合，不冒充真实原名单/PIT或经济收益。

## 4. Contracts / D特征与数据合同，不压缩或补齐

每D核心要求20个截至D连续权威交易日及紧邻下一T；板块要求21个截至D连续交易日。因此组合输入需要21D+下一T，共22个calendar节点，调用核心只取最后20D+T；板块读取21D的close并计算20个连续return的sample std(ddof=1)，ret5必须最后六个close。不得误用20close、压缩缺失日、当前成员、零填或用T行情。

分类仅使用原候选在D前已知的classification_l2_code/known_from；未知成员保留，已知时钟晚于D、重复/矛盾或外部symbol fail closed。SW2021结构crosswalk只翻译行业到指数，不证明历史公司成员；保留134个显式结构pair及code map身份，index id=0是合法值，不当作缺失。板块close只使用(date,index)唯一报价，不使用H5 instrument列重新给公司分行业。

输入全部事前声明来源/字段/单位、实际窗口和hash。文件消费用实际读取字节核对外部pin/预算与前后漂移，单批D的source receipt分别绑定确切键集、calendar、raw/sector来源与feature数值；source或内存model漂移fail closed，不自动刷新或重载。计算hash只证明内容，不证明该历史时点已知；今日数据库补录与capture时间均不能伪装原生PIT。新每日数据只读活动profile已发布公开合同或公开只读源，不能因旧研究loader更方便回退旧profile、训练环境、历史路径或当前行业快照。已有日频DB记录可以作为来源，但必须存在明确表/schema/知识时钟/读取合同后才实现；本文不猜造板块数据表或让数据准备窗口做业务验证。

正常停牌、缺bar、缺板块报价、warmup及无分类仅把相应原候选价格建议置UNKNOWN，不删除或阻断整张名单。common原8字段的停牌归一化与新增raw/sector未知语义分别保留；所有原candidate/group/rank及日期完整，原core12值或sector3值任一必要值未知时，不切换matched/规则建议。UNKNOWN不是模型SKIP，不用0价格/假收益代替。

批量只合并读取，不另写计算。至多20D块：原候选至多400条；各候选自己的20D raw键最多8000、sector各候选所需21D date/index去重键最多8400、各22节点calendar联合最多440。预算以实际去重键集预先核定；LIMIT+1超限失败，不能截断。禁止universe×日期交叉扩表、逐股SQL或每D复制工作区。单D与批量调用同一纯core，在同一输入下必须同值/hash；不同块快照如实记录。

所有SQL readonly/repeatable-read/参数化/finally rollback；sector source不得调用带upsert的model-state GET或默认refreeze/资产补齐接口。当前父腿、预处理器及学习到的组合权重information_end须分别证明<D；模型权重存在、预测文件日期、capture时间或data_split为空不能满足该时钟。缺证明属于UNPROVEN，不自动新QE训练或关闭价格研究方向。

## 5. 价格集合与建议动作

法律raw-CNY范围/原D可见价格坐标/复权声明/tick复用既有监管价格合同，D可见下一T只是时钟。未证明价格坐标或法规上限返回QUERY_DOMAIN_UNAVAILABLE，不能以open±比例或无界价格替代。单候选最多5000个完整法律tick节点，原纯M1 math上限100000只是内部防护；消费者先拒绝超5000，不降采样、插值、扩大训练支持或改变旧81D实际点研究结果。

调用原math，仅candidate用于建议；ACCEPTABLE节点形成一段或多段集合，AVOID可形成无可接受价区间；未知/support洞隔断两侧，不能连桥。集合全未知与确有AVOID但无ACCEPTABLE分别展示，不把未配置/未验证/无支持混成“模型确认今日没有可盈利股票”。所有候选保留，实际无合格建议可以0输出，不形成资金仓位。

原mean头的expected_net_bps不是胜率/盈利概率，path q10/对应downside q90不是置信区间或卖出目标价。保留OBSERVED_PRICE_CONDITIONAL_NOT_CAUSAL_LIMIT_FILL和NAVIGATION_ONLY/deployable=false；不得宣称最佳买点、可成交涨停或此价格带保证收益。卖出价格/Exit模型不由这两个entry头推出。

实际观察价只能查询原D已冻结的格点：off-grid/支持外明确未知，不实时重拟合或重新生成建议。D推荐先于T观察，价格偏离是条件不成立，不以未覆盖开盘判模型失败；超出集合也不机械断言公司出问题。旧完整四臂/UNKNOWN基线研究控制继续原合同，不把UNKNOWN改为现金再沿用旧29.74%结果；产品无建议与研究baseline control分账。

## 6. 研究、确认与正式角色的隔离

独立M1 research loader可核定权重/源码兼容，不证明原native训练来源或新D/PIT。source-only纯core及合成query可继续，不读取sealed/新holdout数据、收益或赢家，不产生新的研究/confirmation登记、fit、包、prediction rows或role。工程合成样本不冒充业务输入。

正式ENTRY_VALUE必须同时有通过的独立确认、真实原训练来源资格、父/组合模型时钟、native每日run/list/pool及独立qualified manifest/role；旧RECOVERED_LIMITED训练原生未证明无法仅靠未来native每日输入或一次确认PASS升级。若这些原历史证明不可恢复，须另行设计合法原生新lineage及训练/确认计划，不能对当前权重倒填native或在本次正候选准备中自动重训。

未来qualified路径按显式family分派，不改变旧v3角色/default NOT_CONFIGURED、不自动捕获/绑定，也不让research bundle当作qualified类型。没有资格时旧Advisory基线继续，价格功能明确UNAVAILABLE；业务不可用原因与股票本身评估分开。API/UI交付、收益确认、用户重启与正式启用各自验收。

## 7. Implementation Plan / 本轮48h内优先顺序

1. #5421源码已合入，按已授权自身官方清理，不rerun旧run或改变CI。原父模型时钟只做已有metadata/公开只读配置核对，真实缺口单列，不控制QE。
2. 本详细设计方法/时钟/工程三视角审核修订及F2校验。源码实现另立精确F1 Card，先交付纯21D+T组合/价格research消费；0fit/0新经济结论。资格链未成立不抢建API/UI或激活平台。
3. 真实source仅在公开原始合同明晰后开发；可用的已消费原候选D用于最小consumer parity，不补市场数据/恢复receipt、不为已负实验复跑。此功能验证不成为新holdout证据。
4. 父腿/组合训练时钟、未知来源资格、独立合法窗口/功效/推断仍按#5420确认框架处理；未完整冻结/授权不运行确认。若外部条件不足，保留源准备状态，不为凑48h搜同窗M5。

## 8. Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-726 | M1真实正NAV/非确认、reader与每日交付及原native缺口分开 |
| F-727 | 显式M1 family/scope、15D+query16、原数学复用及跨包池拒绝 |
| F-728 | 21D+T与core20D一致、原D分类/结构id0/报价时钟及原候选完整 |
| F-729 | 单批同核、有界精确键/只读及正常未知保留/矛盾fail closed |
| F-730 | 法规5000完整tick/支持洞、价格条件非开盘覆盖及非成交/概率/Exit |
| F-731 | recovered训练不升级、confirmation/native/正式role独立且0sealed |
| F-732 | 三切片精确范围、0新fit/不越QE边界及非建议控制分账 |
| F-733 | 多轮自审/直接parity与证据分层、合入重启及生产门分开 |

后续最小直接矩阵为22与21calendar错位/未来毒化、原候选唯一性和15D原纯函数parity、index0/缺分类/正常停牌、缺报价不压缩/支持洞、多段/空/全未知、不同包池/recipe拒绝、实际冻结M1合成价集parity、overbudget及不调用fit/source写接口、research无法升级qualified。广回归交必需CI；真实source/业务验收必须用原名单而非只mock。

## 9. Design Acceptance Matrix

本表仅详细设计验收，源码/daily/确认未交付；不以设计合入报告功能完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-726 | §1/6 | artifact: 本文真实状态及#5414/#5421边界 | DESIGN_VERIFIED | none |
| F-727 | §3/5 | artifact: 原sector math/schema与独立family合同 | DESIGN_VERIFIED | none |
| F-728 | §4 | artifact: 21D+T、D分类和结构映射合同 | DESIGN_VERIFIED | none |
| F-729 | §4/8 | artifact: 单批预算/只读/原候选及UNKNOWN合同 | DESIGN_VERIFIED | none |
| F-730 | §5 | artifact: 支持内价格集合及5000格点/观察语义 | DESIGN_VERIFIED | none |
| F-731 | §6/7 | artifact: 来源/确认/正式与0sealed隔离合同 | DESIGN_VERIFIED | none |
| F-732 | §2/7 | artifact: 精确切片范围与UNKNOWN控制分账 | DESIGN_VERIFIED | none |
| F-733 | §8/10/11 | artifact: 直接验证及production分层 | DESIGN_VERIFIED | none |

## 10. Risks / 审核纪律

主要风险是15D计算兼容被当作旧训练身份完成、正NAV被当作正式资格、仅改善可算价点而非可验证收益，以及长任务再次工程化。只实施当前正候选直接需要的最小纯消费者；来源适配、API/UI/角色都按真实条件放行，不扩大治理或复制研究平台。各轮为本窗口不同视角自审，不冒称独立外审。

多轮审核记录：方法轮核对原candidate16/matched13、gap缩放、UNKNOWN控制及利润概率/成交/Exit边界；来源时钟轮修正为21D+T、明确id0有效和原native训练不可倒补，并补实际消费字节/receipt/hash不证明PIT；工程与交付轮核对三小切片、不改冻结数学或旧接口、只在资格成立后做API/UI，修复F2标题索引缺少Non-goals/Contracts。审核结果只针对详细设计，不把未实现部分或父时钟标为已完成；修订后再次校验八项及蓝图一致性。

DESIGN-COMPLIANCE-001逐项：设计完整不冒充业务完整；正常UNKNOWN无假成功/默补/回退matched；原合同不结果后放宽/新增试验救活；所有未实现和来源缺口如实保留；价格集合不是资金/订单/分钟策略，严格守Advisory叶范围。

## 11. Rollout / Rollback / Production Gates

本PR仅两文档，backend_restart_required=false，DB/DDL/DML/profile/依赖/activation/process/QE submission=NOOP；不掩盖此前已披露的model-state GET元数据upsert事件。本设计不调用任何API、原输入或模型数据。后续源码merge、原D功能读回、经济确认、用户重启、binding分别报告；原模型/输入/研究工件不覆盖，停止新消费者调用是默认回退，不删除唯一未合入M5准备。
