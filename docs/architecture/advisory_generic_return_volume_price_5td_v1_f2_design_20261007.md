# Advisory 跨日量价滞后信息买入价格 GP5 v1 F2详细设计

2026-10-07；设计#5658已合入b3a0a612a并自身官方清理22.594秒。研究假设 `GP5-RETURN-VOLUME-LAG-1`的精确九文件源码/多轮审核/9定向/Ruff/L0/F2通过，一次真实prepare、2head fit及四臂评估已结束，当前candidate负、仅停止自身/不激活。累计115研究fit＋1旧index；测试fixture拟合不计入研究训练。SOURCE的PR/合入和自身清理独立处理。

## Background / Goal

固定5TD的daily、minute、volume-path、joint及ordered候选已各一次完成，尚无相对原Top5的净收益增量；joint/ordered已停止，不重跑、改参数、种子、bin、风险阈值或回选旧控制。原Top5同开发面板平均仍有129.0145bps/5TD cohort，不能据overlay失败断言QE包alpha不足。

新问题只检验D以前的跨日量价**时序耦合**：高交易量发生在价格变化之前还是之后、放量下跌后是否反转，能否改善给定入场价的固定5TD净价值与风险预测。不是开盘价coverage、QE因子发现、重新选股或买卖执行。

新颖性：M9已包含volume-weighted close20、signed volume balance19和volume concentration20，不能再把这些汇总换名为新信息；M18已包含收益RMS/相邻下跌能量。本轮只有成交量与**下一已发生日收益**、成交量变化与同日收益、下跌压力与下一已发生日收益的交互。GP5-volume/ordered只包含D当日分钟信息，不包含这个19日跨日耦合块。使用固定daily GP5学习器以隔离新信息，不继续joint/ordered森林的派生搜索。

理论启发：[Campbell–Grossman–Wang原论文摘要](https://www.nber.org/papers/w4193)讨论成交量与收益序列相关的关系；这只支持可证伪假设，不证明A股、此候选人口或价格模型盈利。成交量不是投资者持仓成本、订单流、真实流动性深度或成交承诺。

## Scope / Non-goals

设计PR只写本文和 `advisory_strategy_conditioned_model_blueprint_v1_20260710.md`。设计多轮审核/F2/current CI合入后，最新main独立源码树登记以下九文件，不默认挂载生产：

- backend/services/advisory_model_first/generic_return_volume_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_return_volume_price_5td_source_v1.py
- backend/services/advisory_model_first/generic_return_volume_price_5td_model_v1.py
- backend/services/advisory_model_first/generic_return_volume_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_return_volume_price_5td_source_v1.py
- backend/tests/advisory_model_first/test_generic_return_volume_price_5td_model_v1.py
- backend/tests/advisory_model_first/test_generic_return_volume_price_5td_pipeline_v1.py
- 本设计；上述蓝图。

只读复用原GP5合同、JSON GBDT exporter/query、cohort评估、研究stage/registry，不改其实现或旧artifact。QE包直接消费，不审核父模型时钟、原native或收益资格；package/stock_universe/single_index/index_union只是来源元数据，同D/同价输出数学一致。

不修改QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/workflow/AGENTS；不写数据库、DDL/DML、安装依赖、数据/profile或模型角色激活、服务/他人进程控制。临时X、正式新run F，后端重启user-owned；本离线切片无需重启。UI、BUG-1778公共交付依赖及旧M1 NOT_CONFIGURED不阻本设计和冻结来源研究，不合并其它未交付源码。

## Architecture / Contracts

### 1. 冻结来源、人口与证据边界

唯一基线父输入为已完成daily GP5 `advgp5_cca74d0498942676713c68ad`。新plan引用其preregistered/prepared/trained三个manifest的文件SHA/size，固定原configuration、386D/7720KEY、9字段、标签、policy、完整原decision_dates。模型控制明确使用父bundle的19维candidate，不从多个旧控制中按收益挑选。父prepared rows直接消费，不重新运行Selection、原标签或旧fit。

新输入只读父plan已绑定的calendar、raw_daily、volume_daily三份引用。keys=(trade_date,instrument)，先按原D及20原session请求投影，再解析close/adj_factor/volume_hand；不消费T..H价格、原模型结果或test收益来选择输入。Parquet物理文件可能包含T以后的行：元数据读取不等于特征消费，数值列实际解析仅发生在D请求投影后；不宣称物理I/O绝未接触包含未来行的row-group。

2026-10-07仅源键geometry：train4380原KEY中4000具20价格键/3991具19量键，380calendar暖启；validation1720和test1620的请求键完整。尚未读金融数值，键存在不证明NULL可用、vintage、收益或可学习。正常缺源/停牌/预热保留原股、原D和逐块UNKNOWN，不重建成员、前填、找最近日期或缩短窗口。

来源仍CURRENT_FROZEN_NON_VINTAGE，沿用原恢复证据等级，不制造原生receipt/capture，不把同源hash当跨包经济泛化。旧sealed/未消费holdout不读；新研究仍已消费开发窗EXPLORATORY_SCREEN/NAVIGATION_ONLY。

### 2. 严格日频时序与单位

D至原calendar的紧邻下一session为T；20session收盘c_0..c_19都≤D，19个r_i=log(raw_close_i)+log(adj_factor_i)−log(raw_close_(i−1))−log(adj_factor_(i−1))，i=1..19。此处的“下一日收益”是窗口内部r_2..r_19，绝非T收益或未来feature。

量坐标q_i=volume_hand_i/adj_factor_i，i=1..19，是沿用M9思想的调整坐标交易量**代理**，不是实际持仓/换手率；乘统一100股常数会消掉。因adj_factor还可能包含股息等作用，不将q称为已证明的股本调整成交股数。全部factor和价格来自各历史session≤D，不使用H复权信息。用log(volume)−log(factor)后减窗口最大log量再exp计算归一化权重，zero volume取q=0，避免合法极大/极小数中间溢出。

NULL/quiet NaN正常UNKNOWN；bool/string/Inf/signaling NaN/已知非正价格或factor/负量是实际计算矛盾，typed失败，不把坏数据变0。原请求键重复/原D-T错位同样失败。实际源hash读前后校验，不覆盖变化中的文件。known字段矛盾独立检查，不因同列别处UNKNOWN吞掉。

### 3. 三个唯一新增特征（冻结19/18，不搜索窗口）

令I=2..19、r_bar=mean(r_i,i∈I)，缩放后的q保持非负。固定：

1. `lag_volume_next_return_cov18 = sum(q_(i−1)*r_i)/sum(q_(i−1)) − r_bar`。
2. `volume_innovation_return_cov18 = sum((q_i−q_(i−1))*(r_i−r_bar))/sum(q_i+q_(i−1))`。
3. `downside_pressure_next_return18 = sum(q_(i−1)*max(−r_(i−1),0)*r_i)/sum(q_(i−1)*max(−r_(i−1),0))`。

三量均是日log-return fraction，非年化/预测利润/开盘涨幅或真实资金流。先max|r|缩放进行乘积、求和和恢复，恢复后检查有限。20价格/19量任一正常未知则块UNKNOWN_SOURCE；全部量0则三量UNKNOWN_ZERO_VOLUME_MASS。只有第三项分母0时仅第三项UNKNOWN_NO_OBSERVED_DOWNSIDE_PRESSURE，不假造“下跌压力后的回报为0”；前两项在恒价且有滞后量时为已知0。若只有最后一个收益日有量，第一项滞后量分母0，仅其UNKNOWN_NO_LAG_VOLUME_MASS、第二项仍可计算。窗口中的zero量合法，非零量归一化后因极值下溢要在receipt记encoding精度限制，不能伪称原交易量为零。

可识别性必须有合成对照：价格路径完全相同，把第1和第3收益日的交易量互换，这两日调整close相同且都上涨、均在最早15日内；因此M9的weighted close/signed balance/concentration及原volume5/20不变，D分钟路径、M18及所有价格特征也不变，而两日后续收益方向不同，lag交互改变。该对照只证明不是旧汇总的重复变换，不是实证收益证据。

### 4. 模型、同群体与固定两fit

保持父九字段、原availability规则、成熟监督KEY和原gap支持。可推理条件只由原九字段的既有规则决定，新块正常缺失不得降低原输入availability、删训练候选或全局阻业务。原九字段median及原100bps桶/2.5～97.5%支持直接冻结父recipe；新增三个median从原train钟内、原input available的D行计算，不看Y成熟/validation/test值，全未知字段编码0＋missing=1但不写回源数据。

candidate=12值＋12missing＋scenario_gap/100，共25维；frozen matched=父19维candidate。新mean与path-q.1两head均沿用父固定GBDT200/lr.05/depth3/min_leaf30/subsample1/seed20261006，共2真实fit、1candidate、0新index、0旧控制重fit。监督仍同AVAILABLE、H成熟、原gap支持和原两正比值；目标/损失/阈值不变。两个head共享成熟KEY，禁止由新特征可用性另筛population。

保存非执行JSON并与安装的sklearn1.8.0原输出parity，模型hash包括recipe、两个head、support、训练KEY和源码身份。validation仅固定误差/缺失诊断，不选点/校准/换参数；test毒化不得改变median、权重或support。before_fit分别记STARTED；第二head失败也保留partial，不隐式重跑第一head。最多7720原行、500000价格行、154400量行、5000calendar session，fit≤30min/2threads/RSS和新bundle≤2GiB。

### 5. 买入价集和四臂评价

保留GENERIC_ENTRY_FIXED_5TD_V1：T..T+4原五交易日，不按停牌/复评延长；buy.95bps/sell5.95bps各一次。mean净期望>0且entry-anchored path-q90≤800bps才ACCEPTABLE。假设价支持外UNKNOWN，不把低价无限外推成好机会；多段完整legal tick≤100000、不跨UNKNOWN洞。明确OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL，非因果挂单价值，不建议最佳分钟点/下单量。

query保留原KEY/rank/order及来源、旧unknown/new块unknown、model/label/policy hash；同input/价在不同包与单/多指数池元数据下数学相同。正式状态ACCEPTABLE_PRICE_SET/NO_ACCEPTABLE_PRICE/UNKNOWN_INPUT_OR_SUPPORT/UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE/NO_CANDIDATES/EMPTY_LEGAL_GRID与未配置区分。不是收益承诺。

四臂为new candidate、父daily GP5 candidate作frozen matched、原Top5 baseline、首次固定±300bps rule。每原D五槽，SKIP留空/不补Top6，所有原D和UNKNOWN/UNSETTLED保留；固定5D重叠cohort不能复利成可投资NAV/MDD。比较两个成本后配对增量、TAKE/胜率/盈亏幅度、known干预D和避免亏损−错过上涨；UNKNOWN-only空槽贡献另列，不能记为模型收益。共同完整组只是描述分母，不删除未结算D；有洞时block5区间null，不能压缩日序列。

两增量点值均正且known干预净价值正仅支持继续新业务研发导航，不激活/独立确认；任何一项负仅停止此candidate，不回选规则/父控制、调种子/窗口/阈值或给旧负实验补证。统计区间、多重已消费研究限制和干预次数完整展示，不创建新的包准入/项目停止门。源码完整可交付与模型盈利分开。

### 6. 原子研究与权限

schema generic_return_volume_price_5td_v1，campaign advisory_generic_return_volume_price_5td_v1_20261007；正式root F:/Dev/AIstock_model_artifacts/advisory_generic_return_volume_price_5td_v1_20261007，experiment `advgp5rvlag_`＋planSHA前24。原子preregistered→prepared→trained→evaluated，registry同RISK_MANAGED_ADVISORY/EXPLORATORY_SCREEN/NAVIGATION_ONLY且唯一变量D_19SESSION_VOLUME_RETURN_LAG_INFORMATION。新manifest串接父引用，不替换旧stage/输入。

prepare首次读取新数值但不消费收益选输入；labels只取原prepared绑定列，不再次读取T..H原价。注册和source clean提交后，拟合前/后分别fresh查询QE single/custom_evo/multi-alpha三公开running=0。QE忙只暂缓fit并继续自己的设计/实现，不操作QE；超过30min才半小时检查。已完成同run只读回，partial不得当exact retry再fit。当前113→若完整唯一2fit则115，另1旧index始终分报，内部trees和测试fixture fit不冒充研究trial。

## Implementation Plan

两docs设计三视角审核/修订→F2/diff/current CI合入/自身清理→latestmain独立九文件源码→新source/纯模型/价集→atomic stage/固定四臂归因→多轮修复、失败只节点复测、最后一次稳定小矩阵/Ruff/L0/F2→clean producer→一次新prepare/QE三0/两fit及评估/后核对→真实结果蓝图/currentCI合入/自身官方清理。工程与研究约3～6h估计，不凑时间、不自动开启下一同族搜索。

日频DB输入仍单独待BUG-1778公共分类；本轮不复制DB reader、改公共workflow、添加生产config、挂API/UI或要求重启。正结果也只是下一必要消费者设计的依据，未完成的经济验证/生产状态如实保留。

## Verification Plan / Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-771 | 新时序块与旧M9/M18/分钟信息区别，旧汇总相同而lag不同的合成对照 |
| F-772 | 原KEY/20价格19量/D-T/先请求后解析、原hash、非vintage/正常UNKNOWN保留 |
| F-773 | 三量手算、比例不变、zero量/缺值/坏数/分母0/数值极限，不读取T特征 |
| F-774 | 25维/父9median与support冻结、同成熟KEY两fit、test poison/JSON/partial |
| F-775 | 成本/固定5TD/完整tick与洞、跨包/指数池元数据数学一致、非fill |
| F-776 | 冻结父控制不重fit、四臂/全原D槽、未知与未结算/非NAV |
| F-777 | 两配对增量、known避免亏损/错过上涨、UNKNOWN分账及干预归因 |
| F-778 | 精确scope/最小测试/真实计数、QE串行/X-F/0DB/激活/用户重启 |

## Design Acceptance Matrix

本矩阵已由设计条款升级为源码/定向测试引用，只证明完整离线源码合同，不证明真实prepare/fit、盈利、API/UI或生产启用；未执行研究不能由SOURCE_VERIFIED冒充STUDY_COMPLETED。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-771 | backend/services/advisory_model_first/generic_return_volume_price_5td_source_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-772 | backend/services/advisory_model_first/generic_return_volume_price_5td_source_v1.py；generic_return_volume_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-773 | backend/services/advisory_model_first/generic_return_volume_price_5td_source_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-774 | backend/services/advisory_model_first/generic_return_volume_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-775 | backend/services/advisory_model_first/generic_return_volume_price_5td_model_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_model_v1.py | SOURCE_VERIFIED | none |
| F-776 | backend/services/advisory_model_first/generic_return_volume_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-777 | backend/services/advisory_model_first/generic_return_volume_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |
| F-778 | backend/services/advisory_model_first/generic_return_volume_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_return_volume_price_5td_pipeline_v1.py | SOURCE_VERIFIED | none |

最小源码测试仅三文件共享fixture：源手算与旧汇总相同新lag不同、缺失/极值/请求外未来毒化；一次合成两head模型、父median/support/KEY、JSON与价洞；stage hash/原人口、partial不二fit、四臂known与UNKNOWN分账。没有实现快照、重复参数fixture、UI/整QE/邻接套件。

## Risks / Rollout / Rollback / Production Gates

量价交互也可能只复刻已知效应、噪声很高；因果性/金融解释需谨慎，不能把高量下跌默认当反转。更新过的adj_factor和NON_VINTAGE源、已消费窗口、多次自适应研究偏差均保留；不把扩信息等同增加alpha。只一个固定信息假设，无风险门槛、确认口径或价support放宽。

Rollout只有完整离线研究SOURCE；rollback停止新候选消费，不改旧artifact/数据库/profile/binding/旧M1五review。backend_restart_required=false，production DDL/DML/dependency/data/model activation/process control均NOOP。自身未完成源码/已完成设计、模型结果和运行态分别报告。

## Review / 多轮设计自审

本窗口第一轮核对新颖性：发现M9已含signed volume，去掉最初考虑的重复汇总，只保留跨日滞后/量变/下跌后续三量，不改旧M9或声称其失败是Bug。第二轮PIT/数值：明确下一日只指≤D窗口内、20价格/19量/18pair、第三项无压力不造0、adjusted量是代理及log归一精度；未知不筛父训练人口。第三轮业务/权限：固定父daily GP5控制而非回选更差joint，冻结成本/政策/两fit，正负NAV与经济确认分开，0数据库/公共源码/激活/服务操作。均为本窗口自审，不冒称独立外审。

第四轮实际设计验证：X端合成脚本 `spike_return_volume_identifiability.py` 确认旧四量严格相同（weighted close100.9166667、signed balance0.2173913、concentration0.0763889、volume5/20=0.8333333），新lag交互由−0.00180915变为＋0.00179133；price/最后5D量/D分钟未改，历史数值/收益/fit=0。F2八条/八行/零warnings通过、两docs范围及diff检查通过。这里仍只交付完整设计，不把合成验证称作模型收益或源码实现。

DESIGN-COMPLIANCE-001逐项：①设计完整、SOURCE/模型/上线未完成明确；②正常UNKNOWN/无压力/partial不假成功；③不改五TD/成本/风险/父候选/旧策略与结果；④不加QE包资格、MDE/原native或人工审批门，只有已有输入矛盾和本研究身份核对。源码/真实研究的逐项证据另由后续完整交付更新。

源码三轮本窗口自审：第一轮核对19/18数学与旧M9可识别性、原KEY/支持/成熟pool及D投影；第二轮数值与纯JSON：先修复diagnostics变更可能导致临时fitted hash失效的顺序，显式无滞后量只UNKNOWN第一项，验证正常缺失/精度下溢/极端量与成本一次/多段洞；第三轮stage和归因：父控制不fit、partial第二次失败、原D/Top6不补、known与UNKNOWN分账及非法action state显式失败。首轮三个pipeline节点因复用fixture漏注册其fitted_packet依赖而未运行，修复后只重跑这三个PASS；三处Ruff问题已修复。后续三个新增边界节点（validation不改权重、价格支持洞、非法state）定向PASS。都是本窗口自审，不冒称外审；实际研究仍0，完整稳定小矩阵/L0另执行。

最终稳定小矩阵9项PASS、Ruff无问题、F2八条/八行/0warnings、diff检查通过；官方nox l0 quality0finding、guardrails0blocking（2非阻断项），不运行整QE/UI/邻接套件。源码与研究尚未合并状态；正式研究只在clean producer与fresh QE三0后启动，旧113研究计数不因测试改变。

## 当前一次研究结果（上述113/未启动均为拟合前快照）

clean producer `8e263673858f2edd38b72429e0aad926fd5591bf`、implementation SHA `42a707dc903ac8c0a9c9e1c6a3e1e0588ac62e351c1b985fee8401327a7dd580`。run `advgp5rvlag_c1869cf9c60177309f17330f`，plan SHA `c1869cf9c60177309f17330ffebe4fe499e734c953331e6710f7d6dcd0910b62`，首次prepare6.469秒：386原D/7720原KEY全保留，7330新三量完整、380暖启/10正常缺源UNKNOWN，0数量编码下溢/未来feature数值解析/SQL/父重fit。没有读新的sealed/holdout或重新选择输入。

2026-10-07T05:38:53.431771及05:38:57.606259 UTC，拟合前/后QE三个公开入口全0；唯一两head fit阶段3.953秒、train至完整评估4.828秒，累计115研究fit＋1旧index。原共同成熟train3684/193D、KEY SHA `ce2e884033d344253561b6f4ebc95ff08f6a3ad15067ab40d270f03d0ee65e20` 与父完全相同；其中3674具新三量、10正常缺失仍保留、未按新信息过滤。validation1586只诊断，mean MSE0.00380347/path pinball0.00607625/lower-tail0.140605，没有用于选点或校准。

80共同完整5TD cohort（原81D另1未结算null）内base/rule/frozen daily/candidate均值129.0145/131.5724/73.6306/57.7266bps；candidate−base/−matched为−71.2879/−15.9040。215已结算TAKE胜率56.2791%，base403/58.3127%、frozen249/56.2249%；known拒买176次/67D，避免亏损44.9896−错过上涨118.3219=known−73.3323bps。UNKNOWN空槽＋2.0444另列，不归模型；总差−71.2879，对账通过。matched known干预98次/53D，非恒等模型。12UNKNOWN、1不可执行、1未结算分别保留；区间/NAV null，不称可投资曲线或收益确认。

model SHA `e2796cd9f3a080c8d6ff759a0b1edd4467f8d3d92417db4da224b04f79c98fe8`，非执行JSON601091bytes；prepared/trained/evaluated stage SHA分别 `920db898311e1aae3916846aa966d7feaeb9e09d47a51b64a4910d2b3335c1c8` / `5bf0df9e3da922ff1dd5c3e183dfcacb6872265948b9695b5761c3ae019fe30d` / `941843e65937b082b9d22ed34466e8b390541a503868e285628b65572dfbefb0`。必要的一次新结果读回核对四stage、两journal、四registry记录、原KEY/known-UNKNOWN归因PASS，没有重fit或重复评估。

新增跨日量价信息在此固定模型/开发窗口未提供价值，只停止此candidate；不把缺失当根因、包alpha不足或全局不可学，不救阈值/seed/window、不回选rule/旧父模型、不给旧负候选补证。没有将此负模型加入production consumer/binding/UI。下一假设必须重新确认不同经济信息或可识别业务目标，不默认在本三量上继续消融搜索。0QE提交/其它模块修改/DB写/激活/服务或他人进程控制；backend无需因这次离线交付重启。
