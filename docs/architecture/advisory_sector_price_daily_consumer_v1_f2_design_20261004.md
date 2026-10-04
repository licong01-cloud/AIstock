# Advisory M1板块条件价格模型：日频消费者 F2 详细设计

2026-10-05；DAILY_ROSTER_API_UI_IMPLEMENTED_UI_AND_EMPTY_FIX_PENDING，非已合入的完整每日业务交付或收益确认。当前以[QE包直接消费合同](advisory_qe_package_direct_consumer_v1_f2_design_20261004.md)为消费者责任边界：进入策略包的组合直接使用，无父时钟、原native或收益确认准入门。复用共享D消费者推进真实日频功能；效果单独报告，不冒称已证盈利。

## 1. Background / 当前事实与业务目的

R2-M1研究源码#5414已合入且自身清理；冻结开发导航通过，但两个配对增量区间跨零。candidate净收益29.7444%包含36个真TAKE及57个UNKNOWN基线研究控制，不能全部归因模型。原7720键、3505板块可用/3991分类或映射未知/224warmup及RECOVERED_LIMITED、native UNPROVEN保持不变。

冻结模型reader PR #5421已在必需CI run37157479273全绿后合入9cd4e831e18909f62e5d23bd9576d8696b045988；多轮自审、16直接测试及真实四头合成parity通过。只证明冻结研究权重消费，不证明真实每日来源或经济资格。该reader不重新加载训练行情/profile或fit，scope中父模型information_end仍UNPROVEN。两腿权重确实存在，但现存manifest、公开loop config及既有editable config均没有原训练时钟（data_split为空/null），不推导需要重训，也不以当前QE默认split补历史训练时钟。

现有每日v3入口直接读取fitted.request.source_request并绑定EconomicModelScopeV2/九字段及旧qualified类型；M1的SectorPriceFitV1没有request，candidate输入16维，不能按“都是GBDT”强塞旧接口。目标是同一冻结价格条件数学消费严格D可见的新数据，给出支持内可能有净价值的多段/空/未知买入价格集合；不是预测开盘价、最佳分钟或保证成交。

初始PR #5422交付详细设计，纯组合#5423已合入，状态见§8.1。用户最新指令取消父包及历史训练身份的重复准入，允许继续完整日频消费者；未授权sealed/新holdout仍不读取。源码/完整功能/效果分别报告，不把直接使用称为收益已确认。

## 2. Scope / Non-goals / 显式范围及依赖

初始设计PR #5422及进度更新#5425均已合入、原自身树已官方清理。本次方向修订使用独立worktree advisory-qe-package-direct-use-design-20261004，精确五文档范围见直接消费合同§2；没有新增业务源码、模型或公共模块修改。

已批准的范围限于以下三个有独立业务价值的小切片；第1已交付，其余按条件放行，逐切片事前登记而非一次大范围业务PR：

1. 纯D组合与研究查询：#5423已新增`backend/services/advisory_model_first/economic_sector_daily_core_v1.py`、对应同叶测试及F1 Card，原价格函数直接消费兼容已通过；复用common D/sector/math/reader，不改原冻结数学或fit hash，不另排重复实现。
2. 真实来源适配：#5434已合入70b22daa930eb18cf1724a9d77a7e7db310fb8e9并完成自身官方清理，按[F1详细设计](advisory_sector_daily_source_v1_f1_design_20261004.md)交付`economic_sector_daily_source_v1.py`及17直接测试；复用common source不改，直接按既存`market.sw_daily(trade_date,ts_code,close)`合同读取本批所需21D报价。原20候选真实D读回已通过，详见§8.2；不调用Selection或父包推理生产者，不建新数据集。
3. 原daily名单、Advisory服务/router/API类型/价格卡片精确范围已登记于[日频交付F2](advisory_sector_daily_delivery_v1_f2_design_20261005.md)，设计PR #5443已合入；源码已实现，真实单/批日与隔离HTTP验证通过，UI浏览器和当前HEAD CI尚待验收。复用既有展示，不等待父包时钟、原生训练身份或独立确认，不新建审批/资格系统。

第2已交付；第3中的分类/family切片写范围已登记于[显式family F1](advisory_sector_daily_family_v1_f1_design_20261004.md)，精确两源码、同叶两测试、F1、本文及蓝图七文件，不含router/API/UI。该切片PR #5442已合入d04ba01f3f2c2c786d6c99529c06813fc35a11ff并官方清理，真实业务链及27直接测试通过，详见§8.3；随后名单/API/UI的实施与待验收见§8.4，不称完整每日功能已交付。不再要求先取得额外包资格才实现，不修改QE、Selection、StrategyPackage、HMM、公共数据、Paper/Execution、公共CI或服务；无候选重建、DB写入/DDL、profile/模型激活或依赖安装，后端重启仍用户执行。消费者不替代QE生产者研发。

## 3. Architecture / 明确family，不复制平台

原run/list候选及D/T/包/政策/池身份→只读有界原始输入→`build_economic_daily_feature_core_v1`的12个D值→`sector_dynamic_rows_v1`的3个D值→显式M1 scope/15D内容记录→法律价格格点query→原`sector_price_set_v1`/`sector_nodes_v1`→价格建议集合。该family接入是功能实现，不另设qualified生产准入路径。

12+3=15个D值，最后第16维是价格查询的actual_gap_bps坐标，不是T的实际开盘或未来行情；matched模型13维只作冻结对照，不回选作荐股。原math把gap bps除100后预测，消费者不可重复缩放或替换成百分比字段。query_gap_bps是展示语义名时必须显式映射actual_gap_bps，不修改原模型schema。

M1 scope显式绑定family、15D字段有序名单、原recipe/数学逻辑hash、原model与reader bundle、package/program/manifest、parent policy/value policy/cost、universe selection、feature语义及来源限制。不捏造旧EconomicModelScopeV2或fitted.request，以真实family适配而非伪造类型；旧资格审查不再复用。相同维数/可加载权重不能证明跨包/跨池适用；换包、腿、pool或policy必须核对声明范围，否则报告实际模型输入不兼容，不拒绝该QE包本身。

原训练D块由共享core的已冻结recipe生成，板块块原纯函数计算；实施需核对真实训练recipe声明与共享逻辑hash，并用已消费原输入证明数值兼容，不能只对新输入追加语义sidecar即宣布旧训练身份完整。纯组合测试/合成query只能证明算法组合，不冒充真实原名单/PIT或经济收益。

## 4. Contracts / D特征与数据合同，不压缩或补齐

每D核心要求20个截至D连续权威交易日及紧邻下一T；板块要求21个截至D连续交易日。因此组合输入需要21D+下一T，共22个calendar节点，调用核心只取最后20D+T；板块读取21D的close并计算20个连续return的sample std(ddof=1)，ret5必须最后六个close。不得误用20close、压缩缺失日、当前成员、零填或用T行情。

分类仅使用原候选在D前已知的classification_l2_code/known_from；未知成员保留，已知时钟晚于D、重复/矛盾或外部symbol fail closed。SW2021结构crosswalk只翻译行业到指数，不证明历史公司成员；保留134个显式结构pair及code map身份，index id=0是合法值，不当作缺失。板块close只使用(date,index)唯一报价，不使用H5 instrument列重新给公司分行业。

输入全部事前声明来源/字段/单位、实际窗口和hash。文件消费用实际读取字节核对外部pin/预算与前后漂移，单批D的source receipt分别绑定确切键集、calendar、raw/sector来源与feature数值；source或内存model漂移fail closed，不自动刷新或重载。计算hash只证明内容，不证明该历史时点已知；今日数据库补录与capture时间均不能伪装原生PIT。新每日数据只读活动profile已发布公开合同或公开只读源，不能因旧研究loader更方便回退旧profile、训练环境、历史路径或当前行业快照。已有日频DB记录可以作为来源，但必须存在明确表/schema/知识时钟/读取合同后才实现；本文不猜造板块数据表或让数据准备窗口做业务验证。

正常停牌、缺bar、缺板块报价、warmup及无分类仅把相应原候选价格建议置UNKNOWN，不删除或阻断整张名单。common原8字段的停牌归一化与新增raw/sector未知语义分别保留；所有原candidate/group/rank及日期完整，原core12值或sector3值任一必要值未知时，不切换matched/规则建议。UNKNOWN不是模型SKIP，不用0价格/假收益代替。

批量只合并读取，不另写计算。至多20D块：原候选至多400条；各候选自己的20D raw键最多8000、sector各候选所需21D date/index去重键最多8400、各22节点calendar联合最多440。预算以实际去重键集预先核定；LIMIT+1超限失败，不能截断。禁止universe×日期交叉扩表、逐股SQL或每D复制工作区。单D与批量调用同一纯core，在同一输入下必须同值/hash；不同块快照如实记录。

所有SQL readonly/repeatable-read/参数化/finally rollback；sector source不调用带upsert的model-state GET或默认refreeze/资产补齐接口。父腿、预处理器及组合训练无泄漏由QE保证，Advisory不再次要求information_end/fit_end证明；data_split为空、原conf不可取得或历史receipt不完整均不阻断直接消费，也不据此要求QE重训。Advisory自己的特征仍只读取D及以前真实输入。

## 5. 价格集合与建议动作

法律raw-CNY范围/原D可见价格坐标/复权声明/tick复用既有监管价格合同，D可见下一T只是时钟。未证明价格坐标或法规上限返回QUERY_DOMAIN_UNAVAILABLE，不能以open±比例或无界价格替代。单候选最多5000个完整法律tick节点，原纯M1 math上限100000只是内部防护；消费者先拒绝超5000，不降采样、插值、扩大训练支持或改变旧81D实际点研究结果。

调用原math，仅candidate用于建议；ACCEPTABLE节点形成一段或多段集合，AVOID可形成无可接受价区间；未知/support洞隔断两侧，不能连桥。集合全未知与确有AVOID但无ACCEPTABLE分别展示，不把未配置/未验证/无支持混成“模型确认今日没有可盈利股票”。所有候选保留，实际无合格建议可以0输出，不形成资金仓位。

原mean头的expected_net_bps不是胜率/盈利概率，path q10/对应downside q90不是置信区间或卖出目标价。保留OBSERVED_PRICE_CONDITIONAL_NOT_CAUSAL_LIMIT_FILL和NAVIGATION_ONLY/deployable=false；不得宣称最佳买点、可成交涨停或此价格带保证收益。卖出价格/Exit模型不由这两个entry头推出。

实际观察价只能查询原D已冻结的格点：off-grid/支持外明确未知，不实时重拟合或重新生成建议。D推荐先于T观察，价格偏离是条件不成立，不以未覆盖开盘判模型失败；超出集合也不机械断言公司出问题。旧完整四臂/UNKNOWN基线研究控制继续原合同，不把UNKNOWN改为现金再沿用旧29.74%结果；产品无建议与研究baseline control分账。

## 6. 研究、确认与正式角色的隔离

独立M1 research loader可核定权重/源码兼容，不证明原native训练来源或新D/PIT。source-only纯core及合成query可继续，不读取sealed/新holdout数据、收益或赢家，不产生新的研究/confirmation登记、fit、包、prediction rows或role。工程合成样本不冒充业务输入。

M1日频接入无需独立收益确认、父/组合时钟、原训练native或额外qualified资格。直接使用已有权重和明确family/recipe；原RECOVERED_LIMITED/native UNPROVEN是历史证据披露，不是消费门禁，不要求另训新原生lineage来恢复资格。不伪造旧receipt，实际每天名单/package/价格字段的错误按正常计算错误处理。

消费者按显式family分派，不把M1强塞旧v3九字段类型。尚未配置/未实现family时如实NOT_CONFIGURED/UNAVAILABLE；这不是对策略包的资格拒绝。research bundle可直接供该family计算，页面/API标明当前效果尚未独立确认，不另造qualified审批。功能、效果、用户重启及配置状态分开，不能把缺功能归因于父包资格。

## 7. Implementation Plan / 本轮48h内优先顺序

1. 已交付：reader#5421、本文#5422及纯21D+T组合#5423均合入并完成自身官方清理；21直接测试及原20候选15D/价格集合功能兼容完成，见§8.1。不再重做这些切片，也不因功能读回另包一层新平台。
2. 直接消费修订：停止核对父腿/processor/组合原时钟，移除preflight二次资格审查和当前producer整体hash/收益门的reader耦合；仅核对真正需要的模型/输入格式与数学兼容。不控制QE、不自动重训、不读取sealed，不再为原conf404搜旧工作区或补证明。
3. 真实source按公开原始合同开发。既有canonical历史价格组件已能支持已消费D的只读研究查询，但当前实时指针是legacy，不能把历史组件可读称为live身份已改变。板块公司分类与行情的来源仍分别核定，不能将结构crosswalk/index membership当作公司分类PIT。实际无法读取的输入只影响相应计算/适配，不追加包资格、不触发补库、激活或旧负模型复跑。
4. 注册M1 family并实现日频/API/UI所需功能，不等确认或历史native；M5作为新的已消费窗口探索可继续，不改旧M1参数或结果。独立效果研究可另行预登记，未授权sealed仍不读；只针对真实功能错误修复，不重复工程验证或搜同信息参数。

## 8. Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-726 | M1真实正NAV/非确认、reader与每日交付及原native缺口分开 |
| F-727 | 显式M1 family/scope、15D+query16、原数学复用及跨包池拒绝 |
| F-728 | 21D+T与core20D一致、原D分类/结构id0/报价时钟及原候选完整 |
| F-729 | 单批同核、有界精确键/只读及正常未知保留/矛盾fail closed |
| F-730 | 法规5000完整tick/支持洞、价格条件非开盘覆盖及非成交/概率/Exit |
| F-731 | QE包直接接受，recovered/nature披露不作资格门；效果与功能分开且0sealed |
| F-732 | 三切片精确范围、0新fit/不越QE边界及非建议控制分账 |
| F-733 | 多轮自审/直接parity与证据分层、合入重启及生产门分开 |

后续最小直接矩阵为22与21calendar错位/未来毒化、原候选唯一性和15D原纯函数parity、index0/缺分类/正常停牌、缺报价不压缩/支持洞、多段/空/全未知、真实模型范围/recipe不兼容、实际冻结M1合成价集parity、overbudget及不调用fit/source写接口、证据披露不升级。广回归交必需CI；真实source/业务验收必须用原名单而非只mock，不测试额外qualified资格审批。

### 8.1 已交付子切片与一次真实原D功能读回

#5423以HEAD93ea35f4b0507fc972341028899586fcf2af0aa9、必需CI37160815000成功后合入e968d2cb84d4765b182e85b8e0ecb7bdae0edbc3，自身cleanup_done。仅新增纯组合叶、同叶测试及[F1 Card](advisory_sector_daily_core_v1_f1_design_20261004.md)三文件；21直接测试/Ruff/F1五项通过。预计算入口消费已有core12D及核定receipt，不迫使再次SQL或重算core；raw入口与预计算入口同核。

最小真实功能读回只使用已消费2024-08-01原20候选、原core receipt、原15D准备值及D前21个交易日板块报价。原H5投影两列59,922行，归一化成168个本候选需要的(date,index)报价；15D数值按rtol=0/atol=1e-12/equal_nan与原M1准备一致，9条完整、11条未知保留。此读回没有打开label、收益、父prediction pickle或新窗口；纯kernel无I/O，但验证runner确实读取已冻结原文件/H5，不能报告整次验证无源访问。

随后复用既有只读价格context源及原sector_price_set_v1：全20条D坐标与原references.parquet相符，完整法律格点最多1491、未超过5000。得到9条ACCEPTABLE研究价格集合、11条UNKNOWN_INPUT_OR_SUPPORT，全部原候选保留；多段集合未连桥。例如601700.SH的三个分离区间为[3.93,3.93]、[3.96,4.00]、[4.02,4.03] CNY。ACCEPTABLE是原冻结模型的价格条件判断，不是实盘荐股资格、盈利概率或成交/收益验证；UNKNOWN也不是模型SKIP。未读取T实际行情或市场outcome，fit/新研究登记/角色激活均0，无需新增包装层源码。

价格源的当前DB历史读回采用READ ONLY/REPEATABLE READ、参数化SELECT与finally rollback，canonical历史组件aistock_equity_pit_canonical_v2已通过原有ready/覆盖/身份检查；实际live_universe_key仍为shsz_st_pit_active_v1、component_is_live=false。首次把live legacy key传给canonical源被拒绝，随后仅按该源公开的显式canonical历史组件合同核对，未更改激活。当前历史D值与原坐标一致不等于历史原生capture，保持CURRENT_DB_HISTORICAL_D_VALUES_NOT_ORIGINAL_CAPTURE/RECOVERED_LIMITED、native UNPROVEN；这些是历史事实披露，不再形成策略包或M1消费的额外native资格要求。新日频消费使用实际公开源，不伪造live指针状态。

原两腿weight的blob仍存在；已有manifest/config无完整fit/processor/学习组合时钟，唯一已明确指向的LSTM Loop9 conf只读文件GET返回404，未尝试其它路径、列举目录、下载params/数据归档或重训。这表示该证明目前不可取得，不证明全局不可学或必须新训练。M1开发结果、reader、计算兼容、原价格坐标消费与正式身份/独立确认分别报告。

### 8.2 真实只读source切片

本次source范围仅新叶、同叶测试/F1及本文状态四文件，核心SQL/行业公共resolver/父包/QE源码不改。每批复用原5个核心SELECT，加2个参数化sector SELECT；独立只读快照如实披露。已消费2024-08-01原20候选，calendar22行、所需sector报价168行，约1.359秒，15D与原M1输入rtol=0/atol=1e-12/equal_nan严格一致，9完整/11UNKNOWN保留。fit/收益/新窗口/DB写入/父包资格检查均0；调用方仍明确提供D可见原公司分类及既存结构映射，不能将此source切片当成已完成分类来源及daily/API/UI接入。

真实读回先发现独立工作树未加载主目录既有环境文件，runner只读取既有配置、未复制或修改凭据。随后发现DB numeric的Decimal不能直接内容hash，以及float64 DB报价与原H5 float32存储产生最高7.4863e-8的sector值差异；已核对H5存储schema，仅在新source按原float32→float64表示投影，不改公共数据/纯math/权重、不放宽parity容差。定向fixture补Decimal及有损小数表示，修复后真实严格parity通过。该工程一致性处理不追加策略包准入、原native或收益确认条件。

### 8.3 分类与显式M1 family子切片

`EconomicSectorClassificationSourceV1`只按原D调用现有IndustryPitResolver公开合同；无release截止日期准入、无当前分类回填、缺分类保持UNKNOWN。`EconomicSectorPriceDailyFamilyV1`消费原source的一次load_batch及真实冻结M1，按实际scope做输入匹配，不用旧九字段类型/qualified角色或matched回退。单批/单日同核、最多20日、每股票5000完整tick，区间附条件net bps和path q90风险，不给概率/Exit/订单。

两叶27直接测试、三轮本窗口自审及真实已消费2024-08-01全20候选链通过：公开D分类+只读DB+真实M1生成9价格集合/11UNKNOWN；15D和全部价格区间与原数学一致，source 7 SELECT、最多1491tick、总约8.516秒。原model hash不变，0新收益/label/T行情/sealed/fit/DB写入/QE提交。详见F1；候选仍由调用方提供，原daily候选DB、HTTP和UI未在本切片实现。历史证据披露不升级，不以此结果宣称模型已盈利。

### 8.4 原日频名单、API/UI和同核历史批量实施

[日频交付F2](advisory_sector_daily_delivery_v1_f2_design_20261005.md)PR #5443合入876316279cfb3118664192ea0abc9333b4980c68后，独立最新main树实现名单/服务/GET及显式M1卡片。2026-08-28真实原published list的47项中20原Top20进行模型计算、27旧持仓/WATCH/范围外项原动作保留；8价格集合/12UNKNOWN，完整约14.969秒。已消费08-24/25/28三日共60候选，批量17.813秒、平均5.938秒/日、特征查询7次，单批价格与15D内容SHA一致，peak working set约419MiB、线程上限2。未读取收益/label/T行情/新sealed，fit=0、DB写入=0、QE提交=0。

原指数准入明确0只与Selection原空分开处理，名单适配允许NO_CANDIDATES；没有原声明不能将缺名单伪装为零。完整空链暴露BUG-1726：共享纯core空object数值列isfinite错误；独立修复29tests/Ruff/L0通过，尚未合入，公共新端点业务smoke语义待流程owner登记。主源码PR #5445的41同叶测试（含6 HTTP）、原生TypeScript及HEAD2261aaa29 CI37219771144 SUCCESS；另有实际冻结M1/只读DB→完整ASGI JSON200，20候选/27未估值，8价格集合/12UNKNOWN、约6.875秒。六项最小UI已编写但未执行浏览器，新文档HEAD检查另核；未合入/用户后端未加载，测试配置仅X。详细矩阵见交付F2 §8.1，真实非空业务不冒充空链或收益确认，不为旧失败候选追加证据。

## 9. Design Acceptance Matrix

本表仅详细设计验收；reader、纯组合与真实source子切片已交付，分类/显式family真实原D功能通过，完整daily/API/UI仍未交付；原native不倒补，模型收益未独立确认，这两项不是功能准入条件。不以设计或子切片合入报告整项功能完成。

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

主要风险是把直接消费误报为收益保证或补齐历史native，以及研发再次被重复资格与平台工程阻断。实现真正需要的M1 family/来源/API/UI并展示真实效果级别，不等待父时钟或独立确认，不扩大治理/复制平台。各轮为本窗口不同视角自审，不冒称独立外审。

历史设计审核记录：方法轮核对原candidate16/matched13、gap缩放、UNKNOWN控制及利润概率/成交/Exit边界；来源时钟轮修正为21D+T、明确id0有效和原native不可倒补；工程与交付轮核对三小切片及F2索引。旧“资格成立后才做API/UI”的安排现已撤销，不是当前执行条件；保留历史审核事实而不恢复旧准入。

历史进度审核按21项测试/原D15值/全20价格context与9/11状态核对，移除已交付待办。当前方向审核进一步撤销父资格与确认启动条件，保留真实scope/模型数学/历史结果；确认仅可选效果研究，不新增低价值固化/包装任务。均为本窗口不同视角自审，不冒称独立外审。

DESIGN-COMPLIANCE-001逐项：设计完整不冒充业务完整；正常UNKNOWN无假成功/默补/回退matched；原合同不结果后放宽/新增试验救活；所有未实现和来源缺口如实保留；价格集合不是资金/订单/分钟策略，严格守Advisory叶范围。

## 11. Rollout / Rollback / Production Gates

五文档直接消费方向修订#5428、直接consumer#5429、M5#5432、真实source#5434及分类/family#5442已合入并完成各自清理。用户此前重启后/api/v1/runtime-identity返回5c51aac49d7cf951f2835a5e68a04c837f803162，与当时main一致；现有QE包只读preflight无blockers且qualification_rechecked=false。M1 HTTP/UI目前只有本地新源码和隔离测试，尚未合入/用户重启，不以旧后端健康证明新接口启用。调用只读DB、0新fit/QE submission/DDL/DML/profile/依赖/activation/process，不掩盖此前已披露的model-state GET元数据upsert事件。源码merge、原D功能读回、效果、用户重启及配置分别报告，原模型/输入/工件不覆盖。下一项为六场景UI/最新HEAD CI/重复审核，通过后合入及自己清理；不等待原native/收益确认或重复父包资格。
