# Advisory R2-M1条件价格价值一次独立确认 F2详细设计 v1

2026-10-04；本文件是可选的一次独立效果研究框架，不是策略包使用或研发准入门禁。按用户最新[QE包直接消费合同](advisory_qe_package_direct_consumer_v1_f2_design_20261004.md)，进入策略包的组合直接可用，父模型/processor/组合训练时钟不由Advisory重复审查；功效和独立确认不再阻止消费者开发或不同模型研究。目标口径固定RISK_MANAGED_ADVISORY，不改判为ALPHA_RANKING；尚未执行独立确认，不宣称收益已确认或自动运行激活。

## 1. Background / 当前真实进展

R2三不同模型M2/M3/M4各按冻结合同完成一次并负向停止（#5404）；新增动态板块M1是新信息而非继续同13D换loss。run `advsectorvalue_d22f697febf9d36f501e1216`以源码`5afdabd93c75ebc2f52f008d8ff3852fec2a7ee9`完成4fit和81D四臂。源码PR #5414最终HEAD `e25d920923c269df22b11493a85f1fa4011562d1`、新CI `37154810577` SUCCESS，已合入`9ec698502dd91796070d4e6b1e64971c3a5c322a`并完成自身官方清理；正式研究产物保留，后续文档提交不冒充重新拟合。真实结果见[M1详细设计](advisory_sector_dynamic_price_value_v1_f2_design_20261004.md)§15。

100共同估值日candidate/baseline/matched/rule净收益29.7444%/21.3220%/23.1821%/20.5747%。候选减基线/匹配的配对日增量6.9758/5.3007bps，95%描述性block区间[-9.5384,24.2783]/[-2.1165,13.7297]均跨零；五NAV门通过，但只允许CONSIDER_CONFIRMATION_DESIGN_ONLY。候选93episode中36为真TAKE、57为UNKNOWN研究控制；不能将全部29.74%归因于M1或说是沪深300超额。训练1795成熟行/194D，validation765行只诊断；原7720键全部保留，来源仍RECOVERED_LIMITED/native UNPROVEN。

M5源码`16e953938`及18个直接测试通过，正式登记/fit/四臂目前0；此前正M1使入口不放行的安排已由用户最新指令撤销。后续在新源码/登记冻结后允许独立M5研究，仍只消费原开发窗并计累计选择偏差，不冒充本M1独立确认。已发生整批15fit+1索引不清零，旧失败不重跑。

## 2. Scope / 本次精确文件与后续实施边界

初始#5420与功效修订#5426均合入/自身清理。本次方向修订使用`advisory-qe-package-direct-use-design-20261004`独立树，精确五份文档范围见直接消费合同§2；本PR未修改源码、不读取holdout或新增fit。源码后续按已登记Advisory范围分别交付，不把文档修订称为运行能力已改变。

后续源码另立Advisory独立树、先登记最小叶文件范围：仅冻结模型消费适配、只读metadata前置、一次确认编排/直接测试及必要文档。不修改QE、Selection、StrategyPackage、HMM、公共数据、Execution、Paper或CI；不能复制第二套模拟器、价格源平台、注册UI或审批系统。源码范围和验收矩阵必须在实现前补齐；本次不把未来接口列为已实现。

## 3. Non-goals / 权限与证据

不读sealed/新holdout的features、predictions、returns、winner、oracle或统计值；不运行确认、重训/刷新父模型、调用QE训练、不重新选开发点。不得更换seed、价格支持、成本、阈值、窗口、信息块或未知策略救活M1。不采集旧负研究额外证据、不让数据准备窗口承担业务验证。

不写数据库/DDL/DML、补数据、重建候选/股票池、激活profile/bundle、安装依赖、控制其它进程。后端重启由用户；本design-only不需要重启。临时X、正式产物未来F独立identity，不覆盖现有输入/工件。源码、独立确认、自然前向、真实fill、生产启用必须分别报告。

## 4. Architecture / 一次选点后的盲确认

固定M1开发点 → 可选独立效果研究 → 分别报告结果与业务配置。不能在读取同一次独立研究结果后回选M1参数或将该窗口再次称为未消费；工程exact retry保持同输入/代码/模型/规则，不覆盖旧attempt或隐式重fit。M5是另立登记的开发假设，可继续使用原已消费开发窗，但不得把其结果改称为M1独立确认。研究分账不再形成全项目停止或消费门禁。

确认不再训练：只消费原M1的candidate16D与matched13D两组固定JSON heads，连同原Top5 baseline和±300bps rule，同候选/同日历运行现有四臂内核。父策略包alpha及Top20名单仍由该包决定；此设计不扩展到其它包/指数池，不把策略包变更当同一确认。

## 5. Contracts / 模型、来源与时钟冻结

研究记录绑定原M1 plan/trained manifest/metadata/model SHA、实际fit source receipt及loader；12D+sector_ret5/sector_vol20/relative_ret5_sector顺序；原无标签gap support及洞；VALUE_REVIEW_5_V1、成本buy.95/sell5.95bps（扣一次）、tick及800bps风险参考；package manifest/runtime variant、股票池模式及实际输入成员；release/profile、节点URI、候选/run/list/artifact、D-1/PIT、calendar及价格坐标。父模型训练/processor/组合学习时钟由QE负责，不再作为Advisory请求或准入条件。

不要求使用旧data_root路径：新数据release须显式验证与冻结模型信息/单位/股票池语义兼容，不能以路径近似或当前配置补旧身份。RD-Agent身份须profile节点data_root_uri且complete=true。原恢复证据不升级；新的完整receipt必须记录真实生成时间，不倒填historical capture。

Advisory自己的学习和特征读取保持D/T及成熟标签边界；不重新审查已进入策略包的父学习腿。所谓独立效果窗口不能包含已被本研究族开发、诊断或选择消费的区间；未获授权的sealed/新holdout仍不读取。实际候选证券/D/T/package/policy/hash矛盾报输入错误；正常停牌、日内歧义及缺行情保留UNKNOWN/删失，不删证券或缩窗。这些是计算与报告正确性，不是对策略包二次准入。

模型函数只用D及以前；真实T open仅查询该冻结函数的合法价格节点，不消费T后HLC。返回多段价集/空集/UNKNOWN，不以覆盖开盘价为目标，不把模型推断价当真实限价成交或分钟最优时点。

## 6. Source-only可行性阶段与sealed隔离

当前可直接消费现有公开package/source、schema、权重、输入和价格模型；不再搜父包原训练截止/processor/组合证明。实际字段、单位、文件可读性及资源问题按消费者功能修复，不追加资格机制。不得为统计覆盖或oracle打开未授权holdout；已消费窗口照实记录。

此前A/B/C状态仅保留为输入可产性诊断，不作为准入：既存权重及输入可算即直接用；实际缺推理工件由Advisory修消费者；另一个包/池仍分别标记身份，不把它冒充当前M1同包结果。字段未记载如实披露，不转成拒绝策略包、要求QE重训或强制等待自然交易日。

2026-10-04 05:35～05:43历史来源检查（以下缺声明不再是现行准入条件）：

- 原父包仍是`pkg_ma_8ec5e389fa2c5e484a1ac7e9`。只读prediction-ref返回有冻结combined prediction指针（sha `e0c571f65006ba381526389f0d2a4bb0efda00e2b2b6ef42f420ca8bd9fc1463`）；未打开pickle/预测行，URI中的20240702～20260310不是权威新窗口覆盖证明。combined `has_model_artifact=false`只说明该字段未绑定，不说明各腿没有权重。
- 只读assets返回37项，其中模型权重717/718分别为LGBModel conservative/LSTM 10D，sha `5f255fc454f02ace754c7c6bfbd8362b37696f18eb1729a488f942ebe0396620` / `9c65fe85fa1e3e31a544c2f59608f6c295de9b7943c4b04aadef3fc34aac87fc`；现存blob静态长度及SHA已与声明核对，未反序列化。factor_schema683（sha `3bde7a5534d0934dadd764277d462a21c676b51f7b175d3fbaf1d396396bcb26`）只声明20个Alpha158DL字段和21D label，不提供各腿information_end、完整processor/schema、组合权重与新窗口完整候选身份。公开components返回0，也不能据此否定legacy组合资产。
- `model-state`响应的active_model_version_id/train_start/train_end均null，因此不能证明父模型PIT。**随后源码核定该GET并非只读**：`backend/services/strategy_package/service.py:get_model_state`会调用repository upsert，更新`strategy_pkg.model_state`检查时间/状态/警告及updated_at。此前本窗口一次调用触发该元数据写入路径，须如实报告为权限偏差；不再调用、不擅自回滚、不修改该模块，未触发训练、bundle/data激活或服务控制。不能把这一实测检查写成“全程DB noop”。
- 当时A/B/C未证明的分类不再用来阻断研发；用户已明确QE包直接可用，权重存在，不继续补腿/processor/组合声明或重训。公共GET是否有写副作用仍按正常操作边界处理；本消费者继续禁用model-state及retrain-preview等有副作用路径。

现存N0窗口合同`advwindow_4edc44f94ec6dfade8bc5494`已将截至2026-03-10的P0C/M1-M5及2026-05-15～07-16历史回放登记为已消费，2026-08-31～11-30为SEALED_UNCONSUMED。这里只读合同的日期/状态，没有打开sealed工件。日期空隙也不自动是合法holdout，须与完整研究族消费记录核对；不能将原test或旧退出缓冲换名再确认。当前profile声明数据截止2026-08-31，不代表9月或上述sealed窗口输入已就绪，也不授权升级数据。

数据准备窗口只处理真实基础数据缺口；候选/政策/业务PIT/可学习及收益验证由Advisory负责。source不成立只报告精确依赖；不得修改其它模块或用旧路径fallback。

## 7. Statistical plan / 功效与经济主终点

原NAV五项和旧结果保持历史原值，不重算改判。此前拟议的两配对单侧95%下界>0、点增量≥5bps及绝对累计/均值相对现金指标，现只用于明确评价目标和效果报告，不再作为开发、回放、包使用或不同模型启动门禁。若另做独立确认，事先说明主终点、区间算法、窗口与支持，不能挑显著对照或混合同；未测指数不称指数超额，开发正点或独立研究也不冒充真实fill。

收益推断以交易日block/cluster为单位，不把1620股票—日期或93episode当独立样本。沿固定block5/reps2000/seed20261004报告配对区间，并在预登记中冻结确认单侧下界的具体算法/版本；总体主终点不依结果选择regime。干预支持仍每对照至少12原决策D及15%，模型TAKE≥30episode，MDD恶化≤200bps、最差5%日均恶化≤20bps。确认前冻结仅由D及以前行情定义的regime及最低覆盖/干预分布；当前尚未核定其数值，不宣称已完成确认预登记，不在盲结果后挑regime。未证明端点/held-mark/未结算时不输出经济净指标。

MDE在任何盲结果读取前核定；两个配对经济点门仍固定delta_min=5bps、单侧alpha=.05，目标power=.80必须明确是单个显著性检验还是完整联合决策。开发期SE proxy为baseline8.7296、matched4.1584bps（100共同日）。原简单平稳正态缩放`N≈100*((z_.95+z_.80)*SE/5)^2`给1884.6/427.6日；120日proxy MDE约19.8147/9.4389bps，仅针对相对零假设、真效应5bps的单个显著性检验，不包含点估计>=5门或绝对收益安全门。它们仅是导航预算警报，不是已验证联合功效、法定门槛、要求自然等1885天或从holdout反算出来的样本量。绝对收益安全门的方差和完整联合功效另行核定，不能用配对SE冒充绝对SE、用各项80%声称联合80%。

功效/样本量用于预算和结论解释，不要求先达到80%才能立项或运行，不设置自然等待天数，也不硬编码为数据库/实验公共默认门。开发窗结果仍标EXPLORATORY_SCREEN/NAVIGATION_ONLY，不冒称独立确认、真实fill或全局方向反证；sealed确认窗口不为探索先消费。

必须区分delta_min与规划真效应delta_plan。在通常连续、近似正态且非零SE的估计下，若真效应恰等于5bps点门，单个点门通过概率约50%，联合概率不会由显著性公式变成80%；因此不能以delta_plan=delta_min宣称整体80%决策功效。完整联合规划须事前分别固定两个合理的delta_plan>delta_min、绝对收益漂移假设及全部判据；这不是抬高假设即可让方案“通过”，更不能将10/15/20bps的假设功效套用到现有6.9758/5.3007bps点估计。规划假设须有经济理由与可核验先验、考虑开发选择与参数不确定性；若没有可信可行方案就保留未就绪，不降旧收益门、拆分总体目标或人为等待多年。

确认结果分类：全部主门/支持/身份通过为CONFIRMED_RESEARCH_NOT_ACTIVATED；效果反证且预注册功效/支持充分可停止该精确candidate；欠支持或欠功效为INCONCLUSIVE_NOT_ACTIVATABLE，不能证明全局不可学。所有结果计入本研究族，不声称仅一次M1就消除了过去选择偏差，不伪造DSR/PBO统计。

### 7.1 已完成的开发期规划诊断（不是新模型或确认）

2026-10-04仅对原M1已消费2025-10-10～2026-03-10的100个共同估值日做一次方法准备。原reader/模型872acff3894c7a64b1b87c51ebd440d739a82069d68be0e30ea27dee9c81931e及evaluated stage字节pin核定；只读取paired_daily.parquet（sha256=56d6a26740c45eb6c2a38804df4bb58e046ef963d11a50b289f2e51ad3412a60），不重放决策、不训练、不读新窗口、sealed市场值或任何新标签/预测。参数在读取前以X盘spec固定（sha256=a9f6920922096dd2605705e7f463e6c0ee2356c9e0ff7549073f85cd4a69c5c8）：circular block5、seed20261004、2000次、假设N=60/120/250/500/1000/2000、配对真效应5/10/15/20bps、绝对真漂移10bps仅作为预算场景。

三维残差为candidate-baseline、candidate-matched及candidate绝对日净收益，同一block索引同步抽样保留相关性；两配对残差相关约0.2835，与候选绝对残差分别约0.4495/0.2825。不能把三项独立相乘，也不能用配对不确定性替代绝对波动。用各N零均值bootstrap均值95%分位作为basic单侧下界的规划偏移，得到以下示例；没有按结果选择效应/天数或改旧NAV：

| 假设日数（非新增真实样本） | 两配对假设真效应/绝对假设漂移，bps | 两配对仅显著性代理 | 加两经济点门代理 | 再加绝对均值下界代理 |
|---|---|---|---|---|
| 120 | 10 / 10 | 31.70% | 31.70% | 10.35% |
| 500 | 10 / 10 | 84.25% | 84.20% | 43.10% |
| 2000 | 5 / 10 | 84.45% | 28.45% | 27.55% |

“三收益检验代理”仍不包含累计净收益、MDD/tail、实际TAKE/干预支持、跨regime与来源身份，所以不是完整policy power或激活证据。源数据仅100个开发日，约20个不重叠五日块的长度等价；实际circular候选块彼此重叠，这不证明有20个独立样本。长期间平稳性与总体方差未知；将其重复抽成2000日不增加独立样本、不验证未来漂移，也不形成自然等2000日的门禁。临界值与曲线来自同一bootstrap池，是plug-in规划估计，不证明检验实际size/coverage；Monte Carlo比例自身最大SE约1.12个百分点。计算约0.344秒、0fit/0DB/0新研究run，不转化为新经济确认或模型trial成功。

结论是修正历史功效规划，不是M1再次失败或全局方向停止。三个案例显示短窗与旧联合判据的影响；可选独立效果研究的窗口、推断细则及regime仍待设计，但不要求80%或父源训练时钟才继续研发/消费。不得因为某个代理情景看起来好就消费sealed或声称已独立确认。

## 8. UNKNOWN与真实价格建议必须分开

原研究四臂在市场可执行而模型UNKNOWN时沿基线动作，作用是控制缺失信息；这是研究控制，不是模型发出的买入建议。当前36真TAKE/57 UNKNOWN须始终分列，不能以总组合盈利宣传M1、补价格或把 UNKNOWN 当安全低开。

未来每日消费者对UNKNOWN必须明确“无可验证价格建议”，不输出默认范围、规则代替模型或自动资金动作；其他Selection基线展示若继续，须明确它不属于M1价格建议。推荐记录/API/UI应绑定角色合同与model/policy/source身份，价格收益是条件估计，path-q90未校准时须标参考风险而非保证概率。

若产品决定UNKNOWN留现金、不输出股票或改变退出/仓位，这不是原研究控制语义。必须先冻结新的动作/政策合同，审查标签及model与policy hash绑定；若标签依赖已变化，不复用旧权重/收益证明新策略，不把29.74%搬过去。该产品合同另立预登记、新lineage及经济验证，不能在本次确认结果出来后切换。动态资金仓位仍需用户扩权，价格建议不研发分钟Execution/Paper。

## 9. Trial registry / 防止重新搜索

复用现有JSONL registry，不新建UI/平台。研究族economic_entry_price_value继续，记录原选定模型及全部父lineage、objective_contract、数据/政策/窗口/来源、study_type、decision_use、0fit/1candidate及结果级别。source-only spike登记为工程检查，不混计DSR模型trial；任何真实研究失败/窗口消费照常记录。

可选独立效果研究使用现存枚举CONFIRMATION / DIRECTION_GATE，表示证据用途而非消费许可；业务启用单独报告。失败不得从同一已消费窗口重新选点并称新独立确认，工程exact retry如实记录。本M1确认方案新fit预算0，不包括另立登记的M5四fit，不重置已发生计数。

## 10. Implementation Plan / 下一执行顺序与预算

M1 #5414已交付，不重做。按直接消费合同先修Advisory重复资格审查和冻结reader的无关producer耦合；同步M5源码、放开正M1前序，重新绑定真实源码与新登记后一次四fit/四臂；M1每日family/API/UI按功能推进，不等待父时钟/native或80%确认功效。原M1权重/参数/收益不改。独立效果研究可另行安排，未获授权的盲窗口不打开，不把开发窗改称确认。

沿总截止2026-10-06 02:36，不重计48h。设计/metadata各≤2h，源码准备在范围审核后≤4h；确认未来0fit，2线程/RSS≤2GiB、X临时/F独立、每30min检查长实验。正式窗口的行数/时间/工件上限在输入metadata确定后登记，不能沿用旧81D假装覆盖新窗。前置不成立只暂停依赖动作，不建设无关平台或收集旧失败证据。

## 11. Verification Plan / 多轮审核及后续必要测试

历史三轮自审核对联合功效与真实资产，发现并披露model-state GET写副作用；旧父时钟/独立确认启动限制已被本次用户指令撤销。当前三视角复审分别核对QE权威与普通输入错误、原收益/模型不可改写及M5新探索身份、精确Advisory范围和无DB/服务操作。功效仅作报告，不构成消费或研发许可。均为本窗口自审，不冒称独立外审。

本次功效修订再次执行三视角自审：统计轮核对delta_min与delta_plan、单项显著性和联合决策的区别，保持原5bps门及原NAV，修正把circular重叠块称为独立源block的风险；证据轮逐项核对原100日parquet pin、固定X盘spec、三维同索引抽样及31.70/84.20/43.10/28.45/27.55数值，明确无新窗口或模型；交付轮修订当前独立树及两文件范围，保留formal source/完整功效/盲确认未就绪，不新增M5择优或公共默认参数。此次是方法准备而非收益确认，仍为本窗口自审，最终F2和diff校验另附PR。

未来直接测试覆盖实际输入/模型hash和范围不兼容、0sealed、冻结模型无fit、支持洞及价格多段、Advisory未来输入毒化、UNKNOWN与控制分账、共同日历/端点mark、预登记/partial/exact retry及不能回选为新独立确认。不再测试父训练时钟资格拒绝。广回归交必需CI，禁止重复快照/fixture堆积；mock-only不冒称真实业务交付。

## 12. Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-713 | M1真实正NAV但非经济确认，M5独立开发研究可继续而非阻断 |
| F-714 | 固定M1/匹配/四臂/包与policy/支持/模型，0新fit |
| F-715 | QE包直接接受，输入可产性诊断不构成父训练时钟审查；sealed隔离 |
| F-716 | 效果/支持/风险与功效诚实报告，不形成研发或消费门禁 |
| F-717 | UNKNOWN研究控制与真实价格建议分离，动作变化触发新政策/标签身份 |
| F-718 | registry合同固定、一次选点、exact retry及三级证据分离 |
| F-719 | 设计/来源/源码准备/盲确认授权顺序，真实缺口不伪造就绪 |
| F-720 | 最小Advisory范围/多轮审核/源码合入与重启及生产门分离 |

## 13. Design Acceptance Matrix

以下只验收本次source-only准备及确认框架设计范围。来源适用性、正式窗口/功效/推断细则、源码与盲确认尚未完成；本设计不报告正式预登记、研究或生产就绪。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-713 | §1/4/10 | artifact: 原M1§15真实run及用户新分流，不改旧结果 | DESIGN_VERIFIED | none |
| F-714 | §4/5/9 | artifact: 固定模型/四臂/0fit合同 | DESIGN_VERIFIED | none |
| F-715 | §5/6 | artifact: QE权威直接使用、输入诊断与sealed边界 | DESIGN_VERIFIED | none |
| F-716 | §7 | artifact: 效果和功效报告不作消费/研发门禁 | DESIGN_VERIFIED | none |
| F-717 | §8 | artifact: 36真TAKE/57控制及新政策绑定要求 | DESIGN_VERIFIED | none |
| F-718 | §4/9 | artifact: CONFIRMATION/DIRECTION_GATE及一次选点 | DESIGN_VERIFIED | none |
| F-719 | §2/6/10 | artifact: source-only准备与盲读取分阶段 | DESIGN_VERIFIED | none |
| F-720 | §2/3/11/15 | artifact: 显式范围及production noop | DESIGN_VERIFIED | none |

## 14. Risks / 不可误读

首次过开发门可能是选择噪声，区间宽、UNKNOWN贡献高、板块known覆盖不足或父包市场阶段变化都能导致确认失败。额外加入新信息后更高组合收益不自动证明更好条件预测或可成交价。设计过于严格也不能变成多年的自然等待：先做合法metadata/功效及产品语义分析，实在不能confirm就诚实分类，不能以省时间消费sealed、拆小同窗择优或宣称确认。

## 15. Rollout / Rollback / Production Gates

当前PR仅文档，backend_restart_required=false；文档提交本身无DB/DDL/DML、依赖、数据/模型激活、进程或QE提交。来源spike的一次model-state GET发生元数据写入路径，按§6单独披露，不以PR文档noop掩盖运行操作，也不擅自回滚。确认通过后的bundle发布/业务接入/自然前向/activation各自独立验收；后端需要加载时通知用户重启，不自动操作。源码问题只修Advisory最小范围，不改他人模块；输入与历史工件不覆盖。

DESIGN-COMPLIANCE-001逐项：完整design-only不冒充完整实现；统计/经济/源/生产状态分报；UNKNOWN/矛盾/缺失依合同处理而非silent fallback；新确认事前冻结且不放宽旧NAV；所有未来源码及测试范围实现前登记，来源/收益缺口不可用mock或目录名满足。
