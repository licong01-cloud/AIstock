# Advisory 跨策略包固定5交易日买入价格价值 GP5 v1 F2详细设计

2026-10-06；DESIGN阶段。用户选择首版固定5交易日，10/20日后续扩展。新目标身份为 GENERIC_ENTRY_FIXED_5TD_V1，绝非将 VALUE_REVIEW_5_V1 五次有效复评改名；本设计通过不代表源码、模型效果、日频交付或角色激活完成。

## Background / Goal

原日频通用输入F1已通过#5461交付：股票/市场九字段独立于父score、LSTM/FUND腿、package和rank。M25原一次研究现累计99物理fit+1历史index；候选22.0222%低于新匹配对照29.7444%，停止该candidate，不用新的期限救活旧模型。M1日频/API #5445及BUG-1726依旧是工程辅线，不阻断新目标研发。

GP5的唯一新研究问题是：仅靠原候选的D日股票/市场信息及假设入场价格，能否给出固定5交易日期末有正净期望且风险受限的买入价格集合？不预测开盘价，不要求实际开盘落入集合；开盘在集合外仅表示该价不获推荐，不承诺它必亏。模型不是上游选股Alpha，不重新选股；完整0～50原名单保留。训练人口、信息量和动作口径改变后，不能与旧复评模型跨run判胜。

“策略包无关”是输入、目标和模型查询不需要上游模型腿/评分/退出政策，并允许同一共享bundle消费任意已交付包及其股票池。不是断言任意候选来源均可盈利；原包仍决定候选人口，同股D输入与同查询价的数学输出跨包一致，真实外部适用性另报。不要重复审核QE包资格、要求旧native或训练时钟证明；package/pool身份仅作来源元数据，真实矛盾仍报计算错误。

## Scope / Non-goals

本设计来自origin/main 66c46d297，仅写本文、既存通用输入F1产品口径更新及蓝图。设计提交/多轮自审/F2/当前HEAD CI/合入后，再从最新main创建独立源码树。源码实施事前精确登记以下范围，新增叶不默认挂载旧M1：

- backend/services/advisory_model_first/generic_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_price_5td_labels_v1.py
- backend/services/advisory_model_first/generic_price_5td_models_v1.py
- backend/services/advisory_model_first/generic_price_5td_inference_v1.py
- backend/services/advisory_model_first/generic_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_price_5td_labels_v1.py
- backend/tests/advisory_model_first/test_generic_price_5td_models_v1.py
- backend/tests/advisory_model_first/test_generic_price_5td_pipeline_v1.py
- 本文与蓝图。

允许只读调用既存 generic_daily_price_input_v1、非执行JSON树export/predict、价格tick与Advisory研究stage/registry工具；不得修改旧helper数学或构造虚假腿。消费者来源适配代码若确需新增，先在本文登记精确文件及接口，不临时扩大范围。

不改QE、Selection、HMM、StrategyPackage、行业/基础数据、Execution/Paper、CI、公共工作流或AGENTS；不写DB/DDL/DML、不重建名单/股票池或profile、不安装依赖、不自动激活模型/角色、不读取新sealed/holdout、不控制服务和他人进程。只消费已有冻结输入或公开只读数据库行情。X任务临时、F新内容寻址正式产物，不覆盖旧plan/标签/模型。后端重启user-owned，纯离线切片无需重启。

## Design / Architecture / Contracts

### 1. 时间、单位与固定政策

明确“五个交易日”采用入场日计为第1日：D收盘冻结输入，紧邻下一session为T，H为T之后第4个交易session的收盘；T..H共5个原市场交易日。仅日频研究，T开盘为名义观察入场，H收盘为名义估值端点，不是订单/fill承诺或盘中最佳点。A股T+1卖出限制不会被5日端点绕过。不得把停牌压缩为有效交易日，不按实际复评/排名、止损、止盈、父包退出而改变H。H之后不延期寻找好报价。10/20日必须新版本/标签，不在首版并行搜索。

冻结成本buy=.95bps、sell=5.95bps，均仅扣一次，与现有成本值相同但政策hash不同。以D已知的目标日raw参考价格reference_D为锚，所有T..H真实OHLC由调用方显式提供对应的价格坐标转换；D特征不能使用T/H复权信息。label_price_basis标明D_REFERENCE_POLICY_RATIO，调用方提供源hash和转换声明，不凭当前复权数据冒称原生vintage。参考不明、坐标未知和停牌为逐行UNKNOWN，已知价格/坐标矛盾则typed错误。

### 2. 标签与正常缺失

纯 build_generic_price_5td_labels_v1(*, candidates, decision_dates, calendar, prices, references, source_context) 接收完整多D原名单（每D0～50，KEY=D/T/instrument，顺序/rank保留）、严格递增原日历、D参考和显式坐标行情，返回(labels, receipt)。decision_dates显式保留零候选D，不能从非空候选反推全研究日期。最多7720候选、5000原session、500000行情行；source_context声明calendar/prices/references来源hash、label_price_basis及证据等级。无I/O/系统时钟/Selection复评。每股D生成一条观测标签，不为假设价格网格复制监督。日历来源hash/完整性声明保留；函数只能验证传入session一致性，不能把调用方漏掉交易日的日历自动证明正确。

输入行情列为 decision_as_of_trade_date/trade_date/instrument/open/high/low/close/d_anchor_factor/suspended/tradability_unknown/up_limit/down_limit；真实raw价格和法规字段不造值。因每个D复权锚不同，行情唯一键包含D，不能把同股日的一个转换因子错用到其它D。每个D参考列为KEY/reference_cny/reference_visible_through，reference_cny是在D锚坐标下的已知目标参考价；可见时钟须<=D。每D T与原日历相邻，原候选唯一性与群体rank完整；无未来日期、重复股日、外来证券/错误来源hash。使用各日raw OHLC乘 d_anchor_factor 后除以该D reference_cny，得到 gross_terminal_ratio=H_close/reference_D 和 path_min_ratio=min(T..H low)/reference_D；observed_gap_bps=10000*(T_open/reference_D-1)。比值均同D锚坐标，路径低值包括T开盘以后的当日低价；限价检查使用raw价而非调整价。label允许消费H内已发生公司行动以正确结算历史回报，但不允许把这些转换回灌D特征或假称当时已知。转换声明缺失保留UNKNOWN_COORDINATE，不从未知新profile补造。

label_information_end=H，label_contract/policy_sha256必须进入标签、plan、bundle与预测结果。已知T停牌或T open>=up_limit保留ENTRY_NOT_EXECUTABLE；法规未知为UNKNOWN_TRADABILITY。H停牌、H close<=down_limit或法规未知为终点UNKNOWN，不能声称可结算净收益。T..H任何正常缺bar/价格/坐标、途中停牌使完整最低路径不可证明时，标签保留UNKNOWN_PATH；不前填或删除原候选，不利用后来数据填补。

H超出来源截止为IMMATURE，不使用未来读取验证输入是否可推理。端点名义mark不等于真实成交；完整可观察路径和法规也不证明真实fill。成熟监督须H<=train_end；train标签缺失正常不fit该观测，但输出计数和原因可追溯，不能删除原研究/评估日期。使用真实actual-open作为一次历史观察，不把label路径作为D feature。

### 3. 九字段缺失与模型表示

直接消费通用输入F1九字段。元数据/原rank/父score/腿、标签状态、收益和未来行情不进入模型。未知基准或宽度不得阻断其它已知股票字段；六个股票字段全部未知时单股UNKNOWN_INPUT，不用包均值冒充观测。其它正常缺失允许训练期逐字段median+缺失标志的显式模型表示；train字段全未知时固定填0并始终missing=1，该0只是模型编码，不写回源bar或声明数值已知。推理及展示保留原unknown原因与掩码。

candidate输入为九字段编码+九missing flags+scenario_gap_bps/100（19维）；D-only matched为同18维且完全不读g。所有median与可用状态仅从train无标签D输入计算；validation/test毒化不得改变median、支持、权重或参数。相同输入/价位在不同包、stock_universe、single_index和index_union元数据下输出相同。模型无需每包再训练才可调用，适用人口和效果限制仍必须展示。

### 4. 固定模型与价格支持

首候选仅一固定模型族GradientBoostingRegressor：200 trees/lr .05/max_depth3/min_leaf30/subsample1/seed20261006，mean平方损失和path q.1两头；matched同参数两头，只移除价格情景g，共4物理fit/0索引/1candidate。不调loss、阈值、seed或持有期救结果，不用父权重。模型保存既有非执行JSON树并要求sklearn原输出parity；不使用pickle/joblib。非正/非有限输出属于模型计算矛盾，不clip成好看数值。

支持域只来自train时钟内真实T价格观察和可用D信息，不看标签收益、未来maturity或分类成功；100bps桶至少30观察/5个不同D、训练gap的2.5～97.5%边界、支持洞保留。这是报价函数的有限观测范围，不是QE包准入或所有股票过滤。支持不足输出UNKNOWN_INPUT_OR_SUPPORT，不将UNKNOWN当NO_ACCEPTABLE_PRICE；极低假设价不得无限外推为“更便宜更好”。

研究前登记全部数据引用/哈希、train/validation/exploratory windows/label_cutoff、原完整候选日期、源码身份、政策hash、四fit预算；源与输入可用性不以收益决定。prepare一次、fit STARTED物理记账，partial不隐式retry；exact运行已完成后不重复fit。临时2线程，fit≤30min/RSS和新增bundle各≤2GiB，只在拟合前后新查QE三公开运行路径均空闲，研究长于30min每半小时检查。

### 5. 查询、集合与输出合同

仅D冻结的输入、bundle、reference_D与调用方显式法律价格上下界；query_price_nodes_v1(*, fitted, features, scenario_gap_bps, arm)逐KEY返回纯节点，generic_price_set_5td_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny)返回集合。价格、参考、上下界和tick均同D_ANCHORED_CNY，若由raw价格转换，tick也须乘同转换系数，不能默认复权价仍以raw .01为格点；输出声明坐标，不直接作为订单价格。单股最多100000完整tick、批次最多500000节点，empty tick显式EMPTY_LEGAL_GRID。支持域内逐点输出expected_net_bps=10000*(mean_ratio*(1-sell/10000)/((1+g/10000)*(1+buy/10000))-1)、downside_q90_bps=10000*max(0,1-path_q10/(1+g/10000))。q10路径不是校准胜率/确定止损。

均值net>0且downside<=800bps的节点为ACCEPTABLE；有已知风险/收益不通过为AVOID；缺输入/支持为UNKNOWN。输出保留所有原KEY/rank/order、model/label/policy hash、持有期起止规则、原来源证据/池身份、原缺失原因和每种状态计数。多段集合不能桥接未知洞；零节点、不在tick法律范围和全UNKNOWN分开。不要求开盘coverage，不以coverage或训练loss证明盈利。

正式文字/机器状态用ACCEPTABLE_PRICE_SET、NO_ACCEPTABLE_PRICE、UNKNOWN_INPUT_OR_SUPPORT、NO_CANDIDATES；未配置模型为NOT_CONFIGURED。集合全无可接受节点且存在未知时为UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE，不能称“已证明无可接受价格”。预测消费者不读label、真实T/H行情或该日未来可执行性。实际T价代入只是readonly后验观察，不改D特征、不返回“最佳分钟”或提交订单。

上述估计是条件于历史观察开盘情景的关联性预测，不是任意挂单/限价fill的因果价值，也不能把同一股票不同假设价当独立样本。开盘-8%可能代表新信息/风险，低价仍可能AVOID或UNKNOWN；开盘+8%也不由固定规则必然拒绝，须由已知价格函数判断。

### 6. 研究测量、效果与独立性

先用本新标签验证新模型，不重用原policy退出模拟器作为fixed5 label。最低100成熟train观测/20D是可拟合约束；不足如实PREPARED_NO_FIT，不是关闭业务或包消费。validation只诊断mean误差、path pinball/低于q10比例、支持/UNKNOWN分布，不回选模型或阈值。确认与探索类别预登记，已有消费窗口只作EXPLORATORY_SCREEN/NAVIGATION_ONLY，不读sealed。

第一轮输出完整原日期的candidate、同群体D-only matched、原Top5基线、固定±300bps规则四臂。为隔离价格判断而不引入新的持仓执行器，采用相同五槽、每个入场日独立等权一次5TD cohort（每臂最多原Top5，SKIP留现金、不补Top6）；不将重叠cohort收益复利成可投资NAV，不称累计资金净收益或MDD。H无法结算的原槽标UNSETTLED，四臂共同完整面板仍保留；不得通过删日期得到完整组合收益。

报告每D/cohort净收益（未知时null及原因）、candidate减matched/base配对增量、真TAKE/UNKNOWN/空槽、干预D、完整/未结算/不可执行数、股票级胜率及盈亏幅度、固定5D目标预测误差。块长5D区间仅描述；重叠窗口禁止独立样本假设。成本后正点增量可导航、不支持激活；区间跨零明确未确认。任何事后调整进入新lineage，旧结果与四fit记账不改判。

跨包检验首分两层：纯数值metadata变更测试证明查询解耦；实际既存多个包及指数池原名单只读业务读回证明消费可用。若现有授权输入只有一个父人口，研究报告单人口，不能伪称完成跨包经济泛化。QE负责新包/上游Alpha，本窗口不运行QE训练扩充数据。独立未消费窗口/自然前向不是功能开发等待项，经济确认状态独立保留。

## Implementation Plan

设计多轮自审/F2/精确三docs/CI合入→独立源码树登记10文件→标签/纯查询优先→同标签四fit模型/轻量cohort评价及已有stage引用→审核修复、失败node先重试、一次稳定小矩阵/Ruff/L0/F2→clean source commit→精确预登记/prepare→QE空闲一次研究→真实结果文档/当前HEAD CI合入/自身官方清理。不让公共smoke/UI阻断本纯研究。

先交付完整离线fixed5买价核心，交付范围严格限于上述实装合同；日频source/reader/API/UI接入需要后续独立精确设计/验证，不修改当前M1 config或自动替换其bundle。独立核心合入后可直接消费真实原名单做只读业务计算；未部署时不能称日频运行生效。如下一阶段需要backend runtime装载，通知用户重启，不由本窗口执行。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-981 | 固定T..T+4五原交易日、新label/policy hash，旧复评与10/20不混用 |
| F-982 | 完整原名单、D/T、价格单位和日历；label与D feature分离、成熟时钟 |
| F-983 | 停牌/限价/缺bar/未成熟保留UNKNOWN，矛盾fail closed，不延长H |
| F-984 | 九字段/18维missing编码/19维scenario、train-only处理，跨包/池数值一致 |
| F-985 | 同监督四固定fit、train-only无收益支持、非执行JSON parity/partial |
| F-986 | 纯成本一次/全部合法tick/多段空集与unknown区分，不预测open/分钟fill |
| F-987 | 四臂完整cohort/重叠依赖/干预与UNKNOWN分报、不伪NAV或跨包经济成功 |
| F-988 | 事前范围、设计/源码/研究/效果/运行分报、用户重启/模块边界及多轮审核 |

## Design Acceptance Matrix

本表只验收设计完整性与边界；DESIGN_VERIFIED不是源码已存在或测试已运行，源码/研究/经济/UI/runtime全部另验。待实施测试仅为合同定义，具体源码阶段须替换为实际测试路径和收据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-981 | §1/2；generic_price_5td_contracts_v1.py（后续） | artifact: 新目标/用户5TD选择；artifact: 本文设计规定的待实施测试 /停牌和复评不改变H | DESIGN_VERIFIED | none |
| F-982 | §2/3；generic_price_5td_labels_v1.py（后续） | artifact: 本文设计规定的待实施测试 /完整KEY/单位/可见时钟/端点手算 | DESIGN_VERIFIED | none |
| F-983 | §2；generic_price_5td_labels_v1.py（后续） | artifact: 本文设计规定的待实施测试 /正常缺失/限价/未成熟与真实矛盾 | DESIGN_VERIFIED | none |
| F-984 | §3；generic_price_5td_models_v1.py（后续） | artifact: 本文设计规定的待实施测试 /test poison/metadata不进模型/缺失原值 | DESIGN_VERIFIED | none |
| F-985 | §4；models/pipeline（后续） | artifact: 本文设计规定的待实施测试 /四次记账/JSON parity/无未来处理 | DESIGN_VERIFIED | none |
| F-986 | §5；generic_price_5td_inference_v1.py（后续） | artifact: 本文设计规定的待实施测试 /全tick/成本一次/未知洞/空名单 | DESIGN_VERIFIED | none |
| F-987 | §6；generic_price_5td_pipeline_v1.py（后续） | artifact: 本文设计规定的待实施测试 /完整四臂cohort/未结算不删日期 | DESIGN_VERIFIED | none |
| F-988 | §Scope/Implementation/Rollout | artifact: 三docs登记；多视角自审/F2/CI在真实执行后记录 | DESIGN_VERIFIED | none |

## Risks / Rollout / Rollback / Production Gates

主要风险：同一人口/已消费窗口偏差；把输入共享当收益泛化；H收盘不可卖却声称结算；全UNKNOWN假空集；开盘情景误当任意intraday成交因果。通过显式label政策、原日期/候选保留、价格/证据声明与效果分报处理，不建设新资格/治理平台。

源码切片默认无人调度、无角色/模型激活；回滚停止新离线调用，旧M1～M25及所有正式工件不动。backend_restart_required=false（本离线切片），DB/DDL/依赖/profile/runtime activation/process control均NOOP；若后续日频接入另立设计，重启归用户。

DESIGN-COMPLIANCE-001逐项：目标、切片与最终业务分别报告，不把input-only或研究当完整产品；正常UNKNOWN/真实矛盾可见，不假成功；旧label/政策/结果不改判，首版5TD明确；所有权/数据库/服务边界不扩张、不新增QE资格审批。不把后续模型有正收益作为提交源码的前置，不通过放宽旧合同宣称成功。

## Review / 多轮自审

第一轮业务/目标自审：固定T..T+4五原session明确，与旧五复评分离；同股D/查询价格数学跨包一致但人口效果不可泛化；价格查询不宣称开盘预测或任意限价成交。

第二轮PIT/单位自审发现同股日不同D锚不能共用单一factor，修订行情键含D、d_anchor_factor和raw限价检查；公司行动未来只用于H内label结算，不回灌D。确认停牌/缺bar/未成熟/终点不可卖全部保留原行与UNKNOWN，不拉长H。

第三轮实现/测量自审：补齐多D输入函数、decision_dates显式保留空候选日、行数预算和查询接口；修订复权坐标tick必须同步转换而不是默认raw .01。四臂cohort允许重叠、不能复利伪资金NAV；未结算/正常UNKNOWN仍保留，SOURCE和收益确认分开。上述设计修订不改旧标签/模型或公共代码，三docs scope核定后再交付设计PR。

自审不是独立外审；F2表格结构PASS不是实装或经济效果。
