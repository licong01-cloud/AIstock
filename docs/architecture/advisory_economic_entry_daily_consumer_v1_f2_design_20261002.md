# Advisory 收益型价格条件每日消费与展示 F2 设计 v0.15

> 日期2026-10-02；2026-10-03状态SOURCE_MERGED_RUNTIME_READONLY_VERIFIED_NO_QUALIFIED_MODEL。父设计：[经济进入价值](advisory_economic_entry_value_v1_f2_design_20261002.md)、[风险与每日身份v2](advisory_economic_entry_risk_alignment_v2_f2_design_20261002.md)、[一致双头v3](advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md)。本设计把离线数值内核接到完整日频条件网格、只读消费者/API与Advisory展示，不新增模型搜索，不研发分钟执行。工程源码/API投影/UI展示验收完成；#5324必需CI成功并合入68ff7aaaa，用户重启后health/identity及三个实际Program默认NOT_CONFIGURED读回通过；当前没有经济确认模型或角色激活。

## 1. Background / 当前事实

v3源码PR #5249已合入`c4ce9566b51d1ff24ad564ef11d27e2344acd4fc`，11直接测试、三轮审核及必需CI run36931373299通过，main已同步。唯一study `advaligned_00089672bee4c91dd2261cf1`有16笔实际模型TAKE、9笔UNKNOWN控制，名义净收益9.6268%而baseline19.1729%，日增量-8.8880bps，95%描述性区间跨零；不确认、不绑定、不调整800/参数搜结果。研究结论不意味着退出价格预测或日常UI已经完成。

当前已有原ENTRY_PRICE日常通道、v2法规价/D身份合同、v3统一条件数值内核和日频数据库源。现有ENTRY_PRICE/M4描述开盘分布，不是经济买价，二者必须共存并明确标识，不互相借用确认receipt或回退输出。

## 2. Scope / 正式交付范围

业务目标为在D冻结信息下，给出T价格条件对应的成本后期望收益和入场锚定下行风险、可接受价格集合及未知范围；集合可为空或多段，不承诺盈利，不输出订单、资金权重、最佳分钟买卖点。收益均值不是盈利概率，不伪造未训练的胜率。

本切片完成网格/消费者/产物/API/UI的成功和失败路径以及历史功能验证；不是只交付NOT_CONFIGURED外壳。经济正增量、独立确认、合格新包训练和正式激活分别验收，当前研究模型不进入实盘建议。不以缺原生历史身份或当前无正模型为由停掉其它可实现功能，不把工程通过当经济有效。

只改§8列出的Advisory文件；旧v1七核心、风险v2五核心及已登记aligned v3五核心不改，保持老reader/hash有效。QE统一上游研究，本任务不改QE/Selection/数据准备/Paper/Execution源码，不训练QE、不重新Selection、不补行情、不写数据库/profile、不安装或控制进程。临时X、持久F，不读sealed；API生效的后端重启由用户执行。

## 3. Architecture / 共核消费

四个逻辑组件按顺序交付，不建立四条实验线：ServingBundle只读适用验证→D日频输入与网格→不可变建议产物→API/UI只读投影。单日与历史批量调用同一`predict_aligned_entry_nodes_v3`；v2风险网格的法规价/身份辅助可以复用，但不能复制第二份数值决策或修改其已绑定源码。

研究与正式产物分开命名空间和路径。真实新v3权重只作为研究NAVIGATION_ONLY消费，所有研究输出明确`deployable=false`。未来正式bundle需新的独立确认凭据，精确绑定同一model/scope/policy/cost/objective；不能把本设计作为当前负结果激活授权。正式资格不能只读取调用者传入的CONFIRMED/deployable布尔值，必须读回已批准评价器产出的完整hash链、预注册合同及通过的结论；当前不存在合格经济确认产物，明确保持未配置。旧research plan的NAVIGATION_ONLY属性不改写，正式资格是独立新产物身份而非修改研究记录。

## 4. Contracts / 身份、时间与状态

### 4.1 ServingBundle与模型scope

绑定真实trained manifest、两份权重、training request/split/source链及实现版本；只读验证后加载，不允许调用者自由声明scope给旧权重补身份。稳定scope包含package/manifest/runtime semantics、九字段顺序及特征语义、shadow policy/cost、价格坐标算法和股票池定义identity。训练全价格panel hash不是未来每日坐标算法hash；字段相同也不等于训练语义相同。

模型scope从实际训练来源及公开冻结合同推导并核验。旧来源无法证明原生股票池定义时明确UNPROVEN，不凭当前包配置或完整每日成员倒填训练身份。当前v3可以在其已消费scope做限制明确的研究；正式模式要求训练适用范围、原生输入及确认均完整，否则typed unavailable。

正式资格mini-contract（只在已有五个消费者文件实现，不新建审批平台）：研究serving manifest保持原状；新qualified manifest引用同一真实aligned plan/trained manifest、训练前准备阶段的`native_applicability.json`、同阶段`feature_semantic_review.json`、独立经济确认的预登记/评价工件及已有trial registry条目。原生适用材料必须是原prepared manifest的既存成员并精确匹配其hash/size；不得训练完成后追加到旧目录。原input identity必须已为NATIVE_COMPLETE，股票池定义hash与其原生universe identity一致，特征语义hash由真实两腿projection、shared builder/bar policy版本、明确PIT universe key/rule version、八D字段、候选/指数20 session及宽度2 session合同推导。语义review绑定同一原feature文件/source，覆盖全部原候选、保留缺失值、差异计数为零。父腿的训练截止必须早于每个用于训练本模型的D分数，不能使用父模型训练期内回填的分数冒充PIT预测。原生每日读取或当前配置不能替代训练资格；若source、builder版本或窗口不匹配则SCOPE_UNAVAILABLE。

经济确认须预登记精确model content（plan/trained manifest hash）、native scope、policy/cost/objective、显式业务风险配置及不可修改的经济门槛/统计规则；经济门槛包括最小决策日、最小实际干预日及覆盖比例、最小成本后日增量与风险退化上界。真实预登记先于确认预测；预测、结算、评价逐阶段parent/hash链闭合，已有registry须含该确认的CONFIRMATION/CONFIRMED/DIRECTION_GATE条目并精确引用工件。后续角色启用另核对已有ACTIVATION/ACTIVATION_EVIDENCE条目，不修改公共registry“只有ACTIVATION能作激活证据”的合同。资格reader验证冻结的匹配日向量、按预登记block bootstrap重算增量区间及风险/干预门槛，不信任调用者CONFIRMED布尔值，也不借用旧coverage合同或探索性CI。确认失败不能反选同一frontier；仅支持既定参数exact retry。当前无合格工件，不启动确认、不读取sealed；未来资格工件的合规生成属于独立确认步骤，不允许此reader拟合、结算、改登记或激活。

确认日历区分决策日和共同组合表现日：前者用于原生D预测、最小干预日/覆盖比例，后者以决策日为前缀，允许预登记最多40个结算尾日，用于完整匹配收益/MDD和block bootstrap；尾日不能记为新决策干预，不删日期或把剩余持仓结算日误作新荐股。门槛同时要求净增量置信下界严格超过预登记最小经济收益、模型成本后绝对收益为正及回撤退化不越界。registry的0/0/0预登记与1/1/1确认计数、scope/schema、同attempt/lineage、完整消费区间及真实时钟均核对；metrics PASS本身不等于确认、激活或因果最优价。

确认授权读回补充（实现前两轮审核）：approved protocol必须引用已有公共`AdvisoryResearchWindowContractV1`、`ResearchWindowAccessRequestV1`和canonical路径的`SealedHoldoutConsumptionReceiptV1`，使用有界metadata reader，只读核对，不调用会生成消费收据的authorize函数。window contract的包/manifest/runtime/三项policy一致，access精确覆盖本确认决策至表现尾日的完整原sealed窗口且同dataset/objective/DIRECTION_GATE；receipt匹配该access、window、policy和唯一candidate，真实contract/approval/consumption时钟不得晚于确认登记。frontier为真实来源study身份；candidate hash绑定model/scope/policy/business risk/criteria/unknown动作，不能结果后变更。两个确认registry阶段须引用这份canonical收据、包含同frontier lineage和window id；同registry已出现另一experiment消费相同frontier则拒绝，原experiment exact retry不受影响。不得换路径或重建receipt绕过一次性选择。

power不是CONFIRMATORY字符串或自报整数：protocol内固定`DEVELOPMENT_BLOCK_NORMAL_APPROX_V1`诊断，引用不可变`ECONOMIC_ENTRY_VALUE_DEVELOPMENT_BLOCKS`测量，绑定开发窗口dataset/日期/完整连续非重叠block均值、预登记block长度、alpha=0.05、target power≥0.8和预期全组合配对日增量（不是每次交易收益）。consumer重算block样本方差、normal-approx MDE与所需表现日数，并核对报告值和calendar支持；测量窗口须包含于contract的非sealed开发窗口且截止不晚于fit label cutoff，不用确认结果估算方差。这是功效前置的近似规划，不冒充最终显著性检验；正式结论仍由冻结block bootstrap/实际干预与经济门槛决定。缺元数据、零方差未解释、数值或窗口漂移均fail closed，不事后放宽。已获批测量的来源真实性仍由独立producer负责，hash/数值读回本身不证明原始市场回放。consumer不生产功效报告、收据或新实验，producer仍须独立获批并真实提供工件。

第二轮时钟审核补充：model_available_at只从原TRAINED registry条目的真实recorded_at读取且精确引用训练manifest，不用label cutoff或文件mtime替代权重可用时间。该时钟必须不晚于protocol批准，避免“今天拟合、倒填去年自然确认”；LOCKED_HISTORICAL_OOT仍可今天拟合/登记但数据maturity和真实原D输入不放宽。此字段仅是消费者加载时的事实，不修改旧serving manifest、权重或工件hash。

每个确认预测日必须引用`economic_entry_native_prediction_day_v1`不可变输入，角色为`ECONOMIC_ENTRY_NATIVE_PREDICTION_DAY`。工件绑定原program/binding/run/list、D/T与真实captured_at、完整原候选顺序/成员、八D特征与内容hash、PIT key/rule、原Selection留档引用及D来源receipt；资格reader读回原留档及package artifact/D-1/hash、完整成员，并核对实际每候选输入，不接受只声明NATIVE_COMPLETE。metadata先核对路径/大小再有界读取，拒绝重复JSON键、非有限数、跨工件内容漂移。qualified bundle可由显式开发者入口在已有合格证据全部验证后原子发布；发布仍deployable=false，不生成confirmation、不创建角色指针、不改数据库或启动日频任务。

首个aligned消费者仍只支持训练时固定800bps风险数值；正式风险必须是独立`EXPLICIT_BUSINESS_CONFIGURATION`，其configuration hash在经济确认中预登记且实际验证，不把研究参考值当用户默认预算。不同业务数值需新的已批准决策合同及确认，不能在本次负结果之后搜索或调整。资格通过≠角色启用：读取合格bundle本身不激活、创建资金仓位或改数据库；日频捕获还须原生source、真实盘前时钟和独立ENTRY_VALUE角色指针获授权。

股票池配置消费QE公开`stock_universe/single_index/index_union`合同。每日成员自然变化绑定当天member hash但不自动要求重训；换包、换指数定义或不同model schema不自动适配。当前九字段中的腿差依赖原有两腿projection；单alpha/其他包不能填假腿分数，只有其自身适用模型和特征合同验证后才可用。本消费者代码通用不等于一份权重对任何策略包有效。

角色消费mini-contract：独立`entry_value_roles/<program>/<binding>/versions/<role_hash>/preregistered/role.json`绑定qualified bundle、同一native scope、经济确认request hash、真实created_at及effective target；`active.json`只读且绑定role/pointer hash、enabled、实际activation时钟/授权引用/registry entry ID。启用记录必须是已有registry的ACTIVATION/ACTIVATED/ACTIVATION_EVIDENCE，精确引用role和qualified manifest并以confirmation为parent；启用不产生模型trial或新消费窗口。资格manifest仍deployable=false，只有已授权角色、当前ENABLED Program、scope一致的原生D产物才能投影CONFIRMED_ENTRY_VALUE/ADVISORY_ONLY；运行时ADVISORY_ONLY不是修改公共研究登记枚举，也不形成资金仓位或订单。GET/构造服务不创建角色、不启用；角色发布、CAS/回滚和真实激活证据属于显式后续操作，不能由只读consumer代办或宣布已完成。

程序与binding在同一有界只读快照解析。推理后使用新元数据快照核对Program/binding，另核角色指针与真实盘前时钟；不重复同一行情快照的generation查询冒充外部新鲜度。每次hook最多处理一个新capture，失败角色隔离并按最近尝试公平推进；未发布原名单为DEFERRED，不当作合法空名单。未来原生D捕获同一原子stage保存`daily_batch.json`及完整`native_d_capsule.json`，含原run/list、D/T/真实capture时钟、八D值、法规价格上下文、成员/原候选及source receipt；读回验证内容/hash对应，不仅保留不可复验的hash。不补旧capture，不赋旧研究模型新身份。

### 4.2 D日频输入

绑定program/binding/run/list、D/T、唯一原候选及完整股票池定义/成员、八D特征内容/来源/可见截止、参考价实际日期/单位、D可见除权公告、ST/board/list-age/tick来源。T必须是权威下一交易日；数据仅≤D，不以T开盘、T行情或成熟标签生成D预测。

每日来源identity与训练identity不同：模型scope一致才可消费新来源，不能要求新日期的行情内容hash等于训练panel hash，也不能只因为日期更新就放宽scope。实际捕获时间真实记录；历史重建时间是现在，不能倒填盘前时间或声明自然OOS。native完整与恢复限制分别保留。自然产物捕获必须在D收市后、T开盘前，实际publication clock、数据就绪凭据和scope均绑定；GET在T开盘后读取标注历史/过期，不把过期建议呈现当前买入指令。候选D/T/symbol唯一、rank为整数且同D唯一，完整原顺序保留。

已消费研究数据若没有可证明的原生run/list引用，两个字段均为null；使用独立`restored_cohort_sha256`绑定原数据集、研究登记、D与精确候选hash，并显式列出缺口。不得生成“frozen_dataset/frozen_cohort”等字符串冒充原run/list。原生路径必须有真实run/list及成员凭据，不能消费这类受限cohort作为完整身份。

无D行的正常停牌候选仍保留，最后有效价记真实日期；跨日除权/参考坐标未证明则该候选UNKNOWN，不把旧价伪装成D收盘。正常缺失不删股票、日期或阻断全批；身份矛盾与缺失不同，program/run/list/package/policy等整批核心矛盾必须fail closed。

### 4.3 风险合同与网格

研究风险只原固定entry-loss q90≤800参考，不接受不同预算做结果后搜索。`risk_budget=null`为RISK_CONTRACT_UNCONFIGURED，不用800默认为用户资金风险。未来正式业务预算有独立配置hash及确认策略，本任务不生成动态仓位。

法规价范围基于D可见信息/公开交易规则；raw CNY、默认1tick，每候选最多5,000节点、每批最多20候选/100,000节点。先算完整节点预算再分配，超限QUERY_DOMAIN_UNAVAILABLE，不截断、加粗step或拿rule区间补位。无日涨跌幅限制时需要另行已批准的有限查询域，未提供就UNKNOWN，不臆造±10%。

只八D字段加hypothetical query_gap进入模型；future exit status、后续OHLC、成熟收益不作为特征。支持内mean>0且风险符合显式合同的节点才ACCEPTABLE；执行未知价格保留独立状态。D推导的upper-limit节点不声明可执行买入，单独EXECUTABILITY_UNPROVEN；其余节点也不保证真实fill。多段集合不跨REJECTED/UNKNOWN节点连接，间隔/格点精度明确，不能对不连续空白插值。“涨/跌8%”本身不是故障判断；若对应可判定REJECTED才建议不买，未知不能冒充SKIP。

节点：ACCEPTABLE / REJECTED / OUT_OF_SUPPORT / EXECUTABILITY_UNPROVEN / RISK_CONTRACT_UNCONFIGURED。集合：ACCEPTABLE_PRICE_SET（注明COMPLETE/PARTIAL）、NO_ACCEPTABLE_PRICE（全域可判定且全拒绝）、PARTIAL_UNKNOWN（有未知且已知段均拒绝）、UNAVAILABLE（没有估值）、QUERY_DOMAIN_UNAVAILABLE或RISK_CONTRACT_UNCONFIGURED。均值误差p90不等于均值置信界，风险coverage不等于胜率。

实际T观察只用于独立验证/消费原D产物：命中冻结格点且可执行才TAKE/SKIP，非格点/域外/未知为UNAVAILABLE，不能寻找最近格点；上限开盘、停牌、S/R歧义保留限制。未来退出状态只用于事后结算限制清单，不改变入场动作。价格条件估值不是改变买价的因果最优解。

### 4.4 产物与公开展示

新版本role=ENTRY_VALUE，objective_contract=RISK_MANAGED_ADVISORY，manifest/advice hash绑定完整source、bundle、D/T、候选、预算及查询精度。JSON必须有限数或null，UNKNOWN不填0。研究路径与正式路径隔离、原子不可变发布、exact retry不重新推理/捕获，不覆盖旧ENTRY_PRICE或研究输入。

完整节点仅保存在有界不可变产物，常规API/UI只投影多段区间、区间内条件mean范围/q90最大值、支持/未知计数、证据与原产物hash，不传输或渲染100,000节点列表。投影有自身schema/hash及producer版本，不把删节点后的JSON误称原完整advice hash。区间mean范围是各条件估值范围，不是区间平均收益、置信区间或跨价格无条件概率。空名单明确NO_CANDIDATES，不查询行情、不制造预测。

正式`evidence_state`只CONFIRMED_ENTRY_VALUE才可作为用户建议；RESEARCH_NAVIGATION明确“探索估值，不用于实盘”，NOT_CONFIGURED/MODEL_NOT_CONFIRMED/SCOPE_UNAVAILABLE保留具体原因。GET读取不捕获、拟合、结算或写产物，不回落旧日/其他包。现有model-shadow增加独立entry_value对象；旧entry_price与其收集状态不改。

## 5. 数据与消费者 / 只读、同义而非重建上游

复用`BoundedEntryReadSession/EntryWorkBudget`、权威交易日与现存原生候选/成员receipt验证，30秒总体预算、单批次readonly repeatable read/rollback、SQL使用剩余预算。数据由数据库或已有不可变输入读取，不调用Tushare/TDX、不修active profile、不生成候选。当前八D字段不含HMM，不能把加载旧HMM父模型或M4 child作为新经济模型的无关前置条件。

八字段必须与训练时Advisory shared_feature_builder的suspension-aware公式逐列同义，复用已有Advisory计算，记录实际语义版本。父分数/名次/腿差从冻结run/list，return/ATR从≤D已有日线，csi300_ret5及market_up_ratio从相同指数/宽度定义；不重新调用QE评分，不以当前策略配置覆盖旧projection。每个缺字段为候选UNKNOWN；冻结源身份矛盾fail closed。不为本功能创建数据平台/通用缓存。

公式相同不等于输入可见窗口相同。每日窄读取目前采用候选/指数20个交易日和宽度两日，宽度两日与既有日频source一致；历史训练panel若使用更长窗口，停牌或缺行股票的前一有效观测及归一化锚点仍可能不同。来源receipt必须分别记录窗口与训练时序一致性状态；未经实际训练语义逐项核验，保持`training_temporal_parity=UNPROVEN`，不能作为正式qualification。名次、布尔占位分数、D/T或腿映射矛盾在DB查询前拒绝；Top20过滤前验证原候选标识与名次，不能把异常行静默过滤为少推荐。

原生候选消费者使用独立冻结projection合同：真实两腿及terminal weights、股票池定义、Advisory review policy（与训练shadow policy分开）、package/manifest/runtime identity；不构造虚假的Ranking/M4 parent来调用旧特征路径。只读核对已经发布的list→review→Selection及run/list共同引用的原留档，校验完整source members、package artifact内容/D-1/成员hash、原捕获时间与唯一名单；支持合法`VALID_NO_CANDIDATE`，但失败或身份矛盾不能当空名单。所有数据库读取用同一有时限的readonly快照；缺原留档不能补建。指数池完整成员若只有原hash没有内容，必须由显式、只读的成员来源提供并匹配原hash，记录此次真实读回时间；不能静默调用旧路径fallback，也不能把后读回的成员说成当时完整留档。原始archive验证与model applicability/训练语义qualification分别输出，不以原生run/list存在代替模型合格。

历史批量预检先选已批准、已消费的窗口和原名单，输入一次加载，单日内核重复投影，按原顺序保留全部候选。旧数据缺D可见法规价/PIT属性时，交给数据准备/Selection窗口最小只读需求；不拿T的stk_limit倒推D范围，也不让本窗口补齐或倒填receipt。

日常自动捕获复用既有`AdvisoryForwardService`每轮完成后的辅助收集hook，新增独立entry_value结果；只扩展该Advisory service的依赖及完成后调用，不改scheduler生命周期/main启动。未配置合格角色时零行情查询/零产物发布；默认不启用，不根据本次负结果开启。配置后按真实D/T和原生已冻结名单捕获，候选未就绪/错过盘前时钟为DEFERRED/UNAVAILABLE，不能重选或倒填。新角色异常隔离，不回滚或修改已完成基线/旧entry_price任务，不通过此hook生成新的数据库写入。

原生archive入口复用确认reader的32 MiB资源上限：先核对声明size、非C原始非跳转路径和实际文件size，再交已有Selection只读publication reader执行一次SHA/字节长度及run/program/binding/policy/名单/runtime合同核验；移除消费者先行的第二次文件hash，不改Selection实现。声明超限或物理size漂移在任何公共read_bytes前拒绝；等长篡改仍由保留的公共SHA校验拒绝。公共reader自身并非有界/严格重复键接口，本修复只证明消费者入口的前置资源检查，不宣称修改或完善了公共reader，也不把JSON语义读回当原生模型资格。

## 6. API / UI

- 新GET `/api/v1/advisory/programs/{program_id}/entry-value/status`：只读当前角色身份、精确目标日/最新合法D产物、原因与剩余资格缺口，不生成预测；运行状态≠业务建议已确认。
- 新GET `/api/v1/advisory/programs/{program_id}/entry-value/research`：显式指定已登记bundle ID/target，仅只读已存在研究产物，验证原program/名单及窗口权限；不接受任意客户端路径、不隐式计算。当前研究v3即使有正价格节点，也只标明未确认探索估值。
- 现有model-shadow加入`entry_value`，采用同一序列化投影。服务失败只影响该新角色typed unavailable，不误改旧ENTRY_PRICE、排名、价格分布或其它正常角色；必需身份矛盾不能隐藏成普通空集。
- 页面为entry_value维护独立请求/加载/错误状态，直接调用status/research API；不得依赖旧model-shadow成功或旧M4/HMM可加载才展示经济价格通道。model-shadow中的同名子对象只是兼容组合投影，不是新角色唯一入口。
- Advisory页新增独立“经济买入价格条件”小组件，显示目标合同、D/T、CNY、证据状态、多段区间/支持范围/未知、期望净收益、entry-loss q90、预算来源及具体原因。正常主推荐不展示探索BUY；研究产物只能在明确研究查看状态呈现。无模型时真实NOT_CONFIGURED，而不是假区间、历史价格或rule_default冒充模型。
- 前端不重新判mean/risk阈值，不把null当0，空集显示“已知条件下无合适价格”，部分未知不得显示“当日全部不推荐”。用户可查看完整identity和限制，默认卡片只展示有业务意义的精简字段。

不改Paper功能、分钟策略或其他页面；虽然Advisory页位于paper-v2目录，写范围仅新Advisory卡片。API/UI不参与执行，未来QE/模拟盘若消费价格条件须通过独立公开合同，由其所属窗口研发分钟执行。

## 7. Historical Validation / 真实证据边界

固定已有v3模型及已消费test日期，不增加trial/model/threshold搜索。历史功能验证比较单日与批量同source/hash/节点集合，D产物不含T outcome，T格点查询与事后执行审计分开；输入缺口逐项报告，不用synthetic fixture冒充真实D网格验证。

真实历史只NAVIGATION_ONLY/RECOVERED_LIMITED，无sealed/OOS激活结论。没有全量native D属性时保留UNKNOWN清单及具体需求，不能声明历史全池已原生复现。功能/API/UI验证可在历史输入完成，不需要等实盘20日；独立经济确认使用合格未消费窗口/自然前向，不能借本历史功能运行取得确认。

### 已消费窗口的真实价格网格功能验证

2026-10-02固定既存v3权重执行 `advefunc_fb9b14be10e2a8450d9931d2`，原D=`2025-10-09..2026-02-02`共81日、1,620候选全部保留，计算413,698个法规价节点。完整捕获29.156秒（平均0.360秒/日），产物114,351,380字节；每个日期关闭自己的只读连接，结束后无遗留连接。535个候选有非空可接受价格集合、1,079个PARTIAL_UNKNOWN、6个UNAVAILABLE，D价格上下文缺失为0。前两个数字描述完整价格域中的条件估值与未知范围，不是盈利股票数或1,620个独立收益样本；本运行未读取实现收益，不能改判v3负增量结果或计算新的胜率。

plan hash=`fb9b14be10e2a8450d9931d22f056156d6fe008a0fdff1f5a3a24a91f34759e6`；根目录 `F:/Dev/AIstock_model_artifacts/advisory_economic_daily_functional_v1_20261002/advefunc_fb9b14be10e2a8450d9931d2`。登记/不可变plan早于此次价格读取，planned/generated/evaluated/selected和新增fit均为0。该功能登记的schema/policy标识是旧bundle/固定政策标签，不是独立确认的完整统计身份；实际模型与原政策以serving manifest和原来源hash链核验。不得把这两条功能登记作为新alpha trial、独立确认或激活依据。原生训练/历史输入限制不变，所有新输出为RECOVERED_LIMITED/HISTORICAL_REPLAY/NAVIGATION_ONLY/deployable=false。

独立进程读回确认81日exact retry零DB重读、零新推理、81个batch hash不变，首/中/末三个真实batch经FastAPI GET序列化及投影hash核验通过，正式角色仍NOT_CONFIGURED；测试应用没有启动用户后端。法规价上下文通过明确的ready canonical数据库组件消费，不需要QE离线profile、训练或候选重建；这不是盘前原生capture证明。

数据依赖必须区分：只读QE摘要为generation=`20260928-v15-unified-moneyflow1`、release=`qe_hmm_full_v2_20260831`、cutoff=`2026-08-31`，本功能运行未消费或切换该离线profile。数据库canonical组件 `aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2` ready且clean、覆盖至2026-08-31，fingerprint=`a8015d9119b5fc8921aff97ea463c05b58927d8609447486bc1b487a9a240949`；活动authority却为DEPLOYED_LEGACY_PENDING_MIGRATION、generation=0、key=`shsz_st_pit_active_v1`。历史受限验证显式记录component_is_live=false，不回退legacy。正式原生路径仍要求ACTIVE_CANONICAL及原lease一致，不能把组件ready等同活动身份完成。此缺口交数据/Selection所属窗口，Advisory不激活、不改指针、不补数据库。

### 历史确认与自然前向的双时钟硬合同

LOCKED_HISTORICAL_OOT允许今天登记、对过去合格未消费窗口预测；原D输入必须已有真实盘前原生capture和完整不可变`economic_entry_native_D_capsule_v1`，预测wrapper引用该原capsule，其八D值/价格上下文/成员/候选/来源必须逐项相同。实际预测时间记录为predicted_at，不能倒填成原captured_at；每阶段记录真实recorded_at，必须登记→冻结全部预测→首次收益读取outcomes_first_read_at→结算→评价，单纯hash链或CONFIRMED布尔值不代替时钟核验。预测时钟可晚于历史T，不因此要求等待未来交易日；原输入仍必须D收市后、T开盘前捕获，数据可见截止≤D且fit label cutoff/上游训练截止早于确认窗口。NATURAL_FORWARD额外要求实际登记/捕获/逐日预测均在相应T开盘前，不可借历史capsule冒充自然前向。缺原capsule、恢复来源、窗口已消费或PIT不明仍不得成为独立确认。只有事先获批的新确认步骤才能读取独立窗口；本长任务没有执行确认、没有读sealed。这是证据时钟修复，不改变旧实验结果、800预算、经济门槛或研究权限。

确认的原生输入捕获不能依赖ENTRY_VALUE已启用，否则出现“先确认才能捕获、先捕获才能确认”的循环。本切片中的自动capsule属于正式角色启用后的未来留档；首次确认producer需独立获批、只读原生来源并保存自己的输入，不调用正式角色自动启用。现有旧样本缺这类capsule时维持受限探索；不能为解除循环伪造留档或启用负模型。独立producer的窗口授权、未消费证明、MDE和frontier一次选点证据仍待完整验收，本qualification reader的单元成功不能替代这些前置。

## 8. Implementation Plan / 精确允许文件

新增：

- `backend/services/advisory_model_first/economic_entry_daily_contracts.py`
- `backend/services/advisory_model_first/economic_entry_daily_inference.py`
- `backend/services/advisory_model_first/economic_entry_serving_bundle.py`
- `backend/services/advisory_model_first/economic_entry_daily_source.py`
- `backend/services/advisory_model_first/economic_entry_daily_service.py`
- 对应`backend/tests/advisory_model_first/test_economic_entry_daily_{contracts,inference,bundle,source,service}.py`
- `frontend/src/components/advisory/EconomicEntryValueCard.tsx`；精确UI测试在现有`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`追加最小参数化展示场景，不新装Vitest/组件测试栈、不复制全套页面fixture。

仅定点修改：`backend/routers/advisory.py`的新依赖/GET/entry_value子对象；`backend/services/advisory_forward/service.py`辅助日常收集依赖/hook；对应现有`backend/tests/advisory_model_first/test_forward_date_clock.py`、`test_forward_api.py`中的最小隔离/响应合同；`frontend/src/lib/api/advisory.ts`新类型与只读调用；`frontend/src/app/paper-v2/advisory/page.tsx`新卡片；本设计、v3设计、主蓝图。AdvisoryForward hook已在设计阶段登记范围，不修改scheduler/startup或数据库基线操作。若实施发现需要其它文件，先记录实际范围和理由，不顺手改其它模块/ownership。

2026-10-03验收进度同步额外登记唯一文件：`docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md`，只更新六UI已经通过和旧消费者可进入CI的依赖状态，不修改十三字段模型接入语义或扩大源码范围。

顺序：设计两轮复核/合入→纯D网格/serving bundle及叶测试→只读source/service与不可变产物→API/UI与精确测试→已消费历史功能验证→源码多轮审核、必需CI/合入→用户重启后的只读语义验证。没有合格模型不激活，但完整消费者源码与功能路径继续实现；任何真实依赖只提出最小需求。

## 9. Verification Plan

定向合同覆盖：模型/scope/manifest/hash漂移、原生与恢复身份区分、未知保留、normal suspend、D/T截止和权威next-day、未来数据毒化、单条/批量同核、非连续区间不桥接、空集合/全部未知区分、预算预分配、非法输出/null序列化、实际格点消费、GET无副作用和UI证据状态。每场景保留一处最小fixture，不写实现快照或重复“大而全”测试；真实历史验证另有compact receipt。

设计至少两轮；源码至少两轮语义审核—修复—定向复验，稳定后最小最终矩阵一次，失败先重跑node。Ruff/TypeScript/相关组件最小测试、diff check、F2 validator及DESIGN-COMPLIANCE-001通过才提交PR。宽API/UI/跨模块回归交CI/Validation Center，不启动新服务。结果分别报告设计、源码、历史功能、API/UI、模型确认、runtime与激活，不以结构validator替代业务。

## 10. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-570 | 真实bundle/model及稳定适用scope，不给旧权重补native身份 |
| F-571 | fresh每日身份、D/T/PIT、正常缺失候选保留 |
| F-572 | 单日/批量共用v3数值内核，不复制业务判断 |
| F-573 | 多段/空集合/部分未知/预算，风险与收益语义正确 |
| F-574 | T格点消费与事后退出限制分开，不倒填D预测 |
| F-575 | 数据库只读有界、同义特征、无QE/Selection/HMM无关前置 |
| F-576 | 原子产物、exact retry、研究/正式路径与证据隔离 |
| F-577 | 独立只读API与旧entry_price兼容，无隐式生成 |
| F-578 | 完整UI精确状态、不伪造利润概率或探索BUY |
| F-579 | 真实已消费历史功能验证、不等待实盘、不升级证据 |
| F-580 | 源码/功能/模型/激活分开，范围/重启/数据权限合规 |
| F-581 | 自有Advisory日常hook、默认不启用、失败不影响基线 |

## 11. Design Acceptance Matrix

设计及源码多轮审核通过；本表仅验收消费者工程，不证明原生训练、模型利润、实际角色启用或用户运行时加载。实际业务功能采用§7既存81日只读网格/API证据及§13六场景UI收据，不重跑旧实验补证。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-570 | backend/services/advisory_model_first/economic_entry_serving_bundle.py | backend/tests/advisory_model_first/test_economic_entry_daily_bundle.py | PASS | none |
| F-571 | backend/services/advisory_model_first/economic_entry_daily_contracts.py; backend/services/advisory_model_first/economic_entry_daily_source.py | backend/tests/advisory_model_first/test_economic_entry_daily_source.py | PASS | none |
| F-572 | backend/services/advisory_model_first/economic_entry_daily_inference.py; backend/services/advisory_model_first/economic_entry_daily_service.py | backend/tests/advisory_model_first/test_economic_entry_daily_inference.py | PASS | none |
| F-573 | backend/services/advisory_model_first/economic_entry_daily_inference.py; frontend/src/components/advisory/EconomicEntryValueCard.tsx | backend/tests/advisory_model_first/test_economic_entry_daily_inference.py | PASS | none |
| F-574 | backend/services/advisory_model_first/economic_entry_daily_inference.py | backend/tests/advisory_model_first/test_economic_entry_daily_inference.py | PASS | none |
| F-575 | backend/services/advisory_model_first/economic_entry_daily_source.py | backend/tests/advisory_model_first/test_economic_entry_daily_source.py | PASS | none |
| F-576 | backend/services/advisory_model_first/economic_entry_daily_service.py | backend/tests/advisory_model_first/test_economic_entry_daily_service.py | PASS | none |
| F-577 | backend/routers/advisory.py | backend/tests/advisory_model_first/test_forward_api.py | PASS | none |
| F-578 | frontend/src/components/advisory/EconomicEntryValueCard.tsx | frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts; artifact: X:/AIstock_temp/advisory-ui-7bbb741e-20261003/receipt.json | PASS | none |
| F-579 | backend/services/advisory_model_first/economic_entry_daily_service.py | backend/tests/advisory_model_first/test_economic_entry_daily_service.py | PASS | none |
| F-580 | backend/services/advisory_model_first/economic_entry_daily_service.py; docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | backend/tests/advisory_model_first/test_economic_entry_daily_service.py | PASS | none |
| F-581 | backend/services/advisory_forward/service.py | backend/tests/advisory_model_first/test_forward_date_clock.py | PASS | none |

## 12. Risks / 不能被工程交付掩盖的缺口

2026-10-03长任务接续复核：最新主线已普通merge进入自有源码分支。两轮审核分别修复“一个Program的既有daily工件损坏中断其他Program”和“空名单没有逐行校验而漏检原生run/list及D/T捕获时钟”；坏工件不修复、不覆盖，按Program保留INPUT_UNAVAILABLE，其他Program可继续，全局30秒budget不能被局部异常隔离吞掉。空名单也必须D<T、训练/上游成熟边界之后、角色生效之后、D收市后至T开盘前真实aware捕获。

复用既有fixture新增artifact隔离分支，另用4项空名单身份/时钟与2项全局budget参数化覆盖直接安全合同，未扩大到其它业务模块或重复大场景。两次先复现再修复、第三次预算边界复核后，当前相关小矩阵62项实际通过（55既有+7新增）；Ruff和diff检查通过。这里的单元/注入工件不能算真实原生捕获、独立经济确认或浏览器证据。之前55项和81日功能记录仍为当时检查点，不作为当前HEAD的宽验收。

2026-10-03流水线已交付专用六场景UI收据，6 PASS/0失败/0跳过/0重试；mock只证明展示合同，配合既有真实受限网格/API和叶测试闭合工程验收，不升级模型资格。消费者#5324已完成必需CI/合入及用户重启后只读默认未配置验证；角色未激活，经济负增量不改判。完整用户功能及新模型效果不能由结构检查、工作树clean或UI单项通过替代。

当前模型净增量负向，风险回撤改善可能部分来自少买留现金，不等于证明风险选择alpha；本任务不补跑现金对照或新模型来挑结果。历史股票池原生身份/特征vintage及D属性可产性仍需消费者核验，metadata/source hash PASS不等于业务正路径已证。日常source缺数据交所属窗口，不在Advisory补写。

本轮不交付卖出价格模型；Exit的下一合法退出vs继续持有剩余价值仍按父设计§16及既有exit oracle/learnability合同后续演进，不把持有期可预测当Exit可学，不把入场价格集合外推出卖出点。

当前源码工程验收检查点：研究serving view、完整D网格、原生候选/D source、不可变batch、资格与ACTIVATION只读消费、日常capture/隔离、独立API/UI已实现，空名单/停用/过期/身份矛盾分别处理。相关稳定矩阵62项此前通过，最新main集成后直接11项通过；81日真实受限功能证据不重跑，四根TS检查保留，六UI已实际通过。工程已通过必需CI run37104100606并由#5324合入68ff7aaaa；用户重启后默认未配置运行验证已通过，正式已配置角色路径尚无真实合格模型可验。首次确认producer、真实合格scope/PIT及角色发布仍独立后续，不由单元工件代办；当前负模型和native UNPROVEN禁止启用，不消费sealed、不增加确认/训练。十三字段新模型的每日路由/完整新recipe不是本九字段消费者的已交付项，按共享内核详细设计另行实现。

以下是实现过程中各切片的历史检查点，不代替上述当前状态：

追加源码审核与修复：独立有界D-only来源已实现单个只读快照内的日线/指数/宽度/停牌/D可见价格属性读取，空名单零行情查询、缺失候选保留真实最后报价日期。原生冻结候选reader已实现program/binding/list/review/Selection/原留档链、真实两腿projection、合法空名单和当日Top20与旧持仓的区分；单指数/指数联合池使用公开成员解析器，但显式指定原lease的PIT key及rule version，不回退默认股票池或旧路径。实际成员读回须匹配原hash，其时间与限制独立记录，不升级历史证据。查询前投影校验及Top20异常行静默丢弃已修复。

第三轮审核修复同一快照内重复generation校验：保留一次行情查询前的身份核对，并明确其仅证明该readonly snapshot，不能冒充快照外最新配置证明；补齐指数rule version和完整候选symbol/rank对应校验。稳定后的五个消费者叶测试24项PASS（6.19秒）、Ruff及diff check PASS；使用注入来源和单元留档，没有真实DB消费证据。训练时序窗口资格仍UNPROVEN；原生输入envelope/不可变捕获成功路径、正式qualification reader、真实历史功能和UI运行仍未完成，不因此请求完整F2合入。

随后完成原生引用的受限研究封装与开发者显式capture入口：先授权已消费目标日再创建只读source，真实run/list保留，八D特征及完整成员/候选键/价格上下文精确对应；所有输出仍RECOVERED_LIMITED/HISTORICAL_REPLAY/NAVIGATION_ONLY。恢复cohort与原生引用研究不能覆盖同一不可变目标日或被错误认作exact retry；两条正向/合法空集路径及未来键、成熟结果混入、无时区时钟、成员漂移护栏两轮审核通过。最新完整叶矩阵26项PASS（6.35秒），不是新经济实验、真实DB或正式捕获证明。前端定向typecheck/syntax再次通过，修正全未知夹具并补上候选唯一性/身份/hash形状防护；三个UI场景仍只collected。宽验证交接草稿未分发，提交版本需绑定实际新增源码HEAD，不拿设计base生成源码PASS收据。

资格切片检查点：资格与角色启用已按公共registry合同分开，训练前原生scope/特征review须为原prepared成员，scope加入PIT key/rule，原生预测工件与Selection留档/package/D-1/成员/八D字段绑定。新增经济确认数值重算、登记/阶段链读回及qualified serving view原子发布；重复发布exact retry，旧恢复来源在任何确认窗口读回/新发布前拒绝，qualification仍deployable=false。当时31个消费者叶测试PASS（6.96秒），其中完整确认链与发布为单元工件/注入来源；并非真实独立确认、真实native训练、DB或UI验收。当时正式capture/角色指针尚未实施；之后的本地实现见当前检查点。固定模型族未新增拟合、未读sealed、未激活角色。

第一轮设计审核修订：补上自动日频收集的自有Advisory hook而不控制scheduler；修正前端测试栈为已有Playwright，不安装依赖；明确独立研究GET路径和不接受任意path；upper-limit执行未知不能出现在可买集合；正式资格读回完整确认链而非caller布尔值；自然捕获时钟、过期展示及候选唯一性补齐。

第二轮逐项审核：核对实际现有forward测试路径，限制hook不改数据库基线；补全空名单、API有界投影及独立projection hash，防止节点爆炸/删节点后冒充完整hash；确认原型两腿特征不适配任意新包、股票池定义/每日成员/训练全panel hash严格分开；确认全价格域未知与拒绝不混淆、停牌逐候选保留；未配置正式模型的真实状态不是宣布完整功能通过。DESIGN-COMPLIANCE-001四项：完整成功消费者/APIUI需实际验收；未知/未确认不静默补位；scope/旧policy/排名/研究结果不漂移；不新增审批平台或等待实盘门禁。

提交前追加集成复核发现：如果页面仅从旧model-shadow读取新子对象，Ranking/M4失败会间接阻断新ENTRY_VALUE。已修订为独立API请求/状态并列显示，源与页面都不得通过旧M4/HMM取得无关前置；后续定向测试必须验证旧角色失败时新角色仍可读。本修订只在已登记API/page范围内，不扩大业务模块或修改旧策略排序。

## 13. Production Gates / Rollout / Rollback

design_accepted=true_design_pr_5250_merged；source_implementation=complete；source_acceptance=engineering_verified_merged_PR5324；api_ui_source_verified=true；runtime_activation=readonly_default_unconfigured_verified_after_user_restart；historical_daily_grid_verified=true_navigation_only；economic_model_confirmed=false；binding_active=false。设计合入commit `f0feb238121569fcfdfd07f6d3b24ad03369f0f5`；消费者#5324已合入68ff7aaaa9358c160c75916231207d5f87f3c014；必需CI run37104100606成功，main已同步并完成本源树官方清理。后端加载目标`backend-main`，用户操作见`docs/operations/backend_main_runtime_restart_runbook.md`；用户重启后已核对merge identity与三个实际Program的独立entry-value GET默认NOT_CONFIGURED语义，不调用有副作用model-state，不启用模型。DDL/DML/profile/依赖/用户进程操作noop，QE训练及sealed访问0。

本次UI收据：run_id=`advisory-ui-7bbb741e-20261003`，HEAD=`7bbb741ebeb5c354cc9bd8e529475a3109a7dc49`，spec SHA=`bd4396aeafc843d5eda8a3c6badcf3b28927cf1dd15c3dbf057626aad25f2f68`；artifact=`X:/AIstock_temp/advisory-ui-7bbb741e-20261003/receipt.json`及`results.json`。原始结果expected6/unexpected0/skipped0/flaky0，最终28.753秒。完整标题前缀导致原`^economic entry`零收集，runner改等价`(?:^| )economic entry`并强制逐裸标题恰为指定6项，未改spec/跳过/扩套件。源码361个Git blob核验、依赖lock匹配、排除.env.local；runner-owned前端/浏览器已退出，全部临时X、默认拒绝未mock网络，无backend/DB/安装。后续合入main仅其他模块文档及本文/蓝图验收记录变更；已核对backend/frontend与该收据HEAD完全相同，原收据不改写、不声称在新文档HEAD重新执行过UI。PR最终源码等价检查另报告；若代码变化须重新验证。

以下55项/六UI仅收集等段落为实施过程历史检查点；当前状态以上段及§11矩阵为准，不再形成旧实验补证任务。

最新稳定门禁为55 passed、30.07秒（51消费者叶+4 API/hook；2项既有其它模块告警未越界修复）。两轮审核新增既有canonical窗口/access/一次性收据只读核验、同frontier不同experiment拒绝、开发block功效/MDE重算及真实TRAINED登记可用时钟；12项授权/功效/双时钟定向节点曾单独通过，全部使用X盘合成工件，不是独立经济确认。新代码重新加载实际旧v3研究bundle，TRAINED真实recorded_at=`2026-10-01T21:44:06.190923+00:00`；20条真实batch投影及旧manifest/batch hash不变、正式NOT_CONFIGURED，零新fit/新收益/sealed读取。§7的81日只读功能证据保持，浏览器/CI及正式启用仍未完成。Ruff/diff通过，四根TypeScript此前通过；最终设计结构须复核，并以实际新增源码HEAD绑定宽验证，不拿设计base冒充源码证据。

本地交付策略：只提交一个明确标注验证中的消费者源码检查点以绑定新HEAD，不据此请求完整F2合入。六个注册浏览器场景仍需安全runner；现有generic Paper UI计划会启动验证后端并运行整个tests/paper-v2范围，不适合本次仅Advisory且不写DB的约束；公共MCP runner又缺请求级TEMP覆盖。须由验证所属窗口提供只跑六场景、无后台/数据库、无安装、临时全X的可核验配置，本窗口不修改公共runner或其它模块。独立confirmation producer、合法新窗口和真实角色发布另行获批；本次只完成这些既有证据的consumer硬校验，不代办生成或激活。

新增展示合同复核：拒绝PUBLISHED空名单、非空NOT_CAPTURED/NO_CANDIDATES、未知节点枚举、节点计数/可接受集合矛盾、非正净收益或超过原风险预算的“可接受”区间；前端只验证响应，不重新选价、不调整800固定合同。保留全部可估值节点仅为执行条件未知时的PARTIAL_UNKNOWN/UNAVAILABLE合法组合，不把未知变成拒绝。修改限原卡片/既有六个UI场景，复用同一fixture。类型错误已定点修正，四根TypeScript类型与语法检查均零错误，六场景重新收集成功；仍未运行浏览器，不能作为UI PASS。后端未变，55定向测试及真实81日证据不重复执行、不改判。

随后两轮留档入口复核修复只修改daily_source及原有同一原生/合法空名单fixture，2节点最终6.24s PASS、Ruff/diff PASS：正向名单/空名单仍可读；声明超限及实际size漂移未触发公共读取，等长内容篡改仍由公共SHA拒绝，所有拒绝都未查询行情。55项是此前最终矩阵，未在此修复后全套重跑；本次重验其中2个直接受影响节点，不加计为57项。数据、模型和81日输出均不变。实时验证中心catalog只提供Advisory backend计划，没有可执行的Advisory专用UI计划；未分发generic计划、未起runner/服务。

源码交付逐项复核（与§11的设计审核状态分开）：

| 设计条款 | 实现位置 | 实际证据与结论 | 剩余影响/下一步 |
|---|---|---|---|
| F-570 | serving_bundle | 旧真实权重/hash/可用时钟读回；研究消费者通过 | 原生训练资格未证明，禁止启用 |
| F-571 | daily_contracts/source | 叶测试核验D/T/身份/缺失保留 | 正式活动PIT与原始lease由所属窗口闭合 |
| F-572 | daily_inference/service | 81日413,698同核格点完成；零新fit | 不证明模型经济有效 |
| F-573 | daily_inference、UI卡片 | 多段/未知/预算叶测试及六UI通过 | 用户重启后默认未配置验证通过；无合格角色仍不可部署 |
| F-574 | daily_inference | 原D格点精确消费与T绑定测试通过 | 不做分钟择时、订单或成交证明 |
| F-575 | daily_source | 实际只读canonical组件消费，非活动身份限制保留 | 仅提数据所属需求，不激活/补写 |
| F-576 | daily_service | 81日exact retry零DB/推理、hash不变；原子和资格叶测试通过 | 首次独立确认producer未交付 |
| F-577 | advisory router/API | 三个真实batch GET投影及定向API测试通过 | 已合入并用户重启，三个实际Program默认状态读回通过；不冒充已配置正式路径 |
| F-578 | EconomicEntryValueCard | 四根TS检查及指定六UI实际PASS，收据见本节 | 仅展示合同，不代表收益确认 |
| F-579 | daily_service/历史功能plan | 81日1,620原候选受限功能通过 | 仅NAVIGATION_ONLY，未读独立确认窗口 |
| F-580 | 自有worktree/设计/精确diff | 工程逐项通过，20文件Advisory精确scope | 必需CI/源合入/源树清理及用户重启后默认状态均已核对；经济确认仍未通过 |
| F-581 | advisory_forward hook | 四个定向API/hook节点通过，默认未配置 | 新源码已加载，三个实际Program不自动捕获；未执行有副作用hook |

DESIGN-COMPLIANCE-001复核结论：①本设计消费者工程完整实现及源/API/UI证据齐备，模型利润和用户运行时不冒充完成；②矛盾响应拒绝、正常UNKNOWN保留、GET无隐式生成；③原排名/模型/成本/风险/窗口不变，新十三字段接入另项；④不新增审批或实盘等待门禁。已有授权允许必需CI通过后提交合入与自身善后，后端重启仍由用户执行。

源码按用户既有授权、多轮审核及必需CI由#5324合入，源树清理完成；用户重启后health=ok、merge identity精确匹配、三个实际Program的只读默认未配置状态通过，未触发研究或角色启用。没有合格ENTRY_VALUE模型不生产绑定；未来正式角色须同时满足模型确认与scope/输入证据合同，不能用本工程PR绕过。

新角色失败仅新子对象unavailable，旧M4/ENTRY_PRICE与基线不改；版本回退只选择已批准的新角色artifact，不覆盖旧模型/名单/捕获时钟、不改数据库、不重启服务。未完成项保持明确状态，不用POC/placeholder或mock-only交付冒充完整日常价格建议。
