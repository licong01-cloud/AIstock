# Advisory 历史隔夜／日内路径 M12 F2详细设计

2026-10-05；DESIGN_ONLY / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

R2一次M11已完成，累计43 physical研究fit+1历史index，源码#5463 HEAD643ebbfe6/CI37249491174 SUCCESS后已合入f6aef87db4e50e1a2b5a2a407f0128ee8b685708并自己official cleanup_done；它相对原baseline日+1.0512bps<原5、尾部恶化46.9917bps>原20，仅停止本candidate，不补证/调参/接family。M1六UI和BUG-1726公共smoke仍仅工程辅线。通用日频输入#5461已合入，独立通用label口径待用户选择；本M12沿原明确VALUE_REVIEW_5_V1，不默选新目标，不伪装通用权重。

H-DAILY-OVERNIGHT-INTRADAY-PATH-1：同一收盘路径可以由不同隔夜跳空与日内涨跌组成，19个历史间隔的两部分方向及跳空幅度，是否提供原条件买入净价值模型未表达的信息？M7只含price_drawdown20、price_path_efficiency19、price_up_day_share19；旧12D的ret1/5/10、ATR、D close_location不包含完整19间隔Open分解。M12读取已存历史raw_open，不叠加M5～M11负块，不换旧模型seed/loss或回选旧控制。不是预测T开盘价，不是实际隔夜套利收益或分钟择时；只检验不同D已知条件下的净价值集合。

事前工程spec仅known-key几何：7720原候选/386D、38168请求对、38158价格对，7330具备所需20session/19Open、380预热、10正常缺行，5.047秒/0SQL/fit/labels/收益数学。不是研究正结果，来源非vintage/native UNPROVEN不升级。

## 2. Scope / Non-goals

设计独立树从当时最新main6bb8dd7a4建立，仅新文件未提交时已ff同步M11合入main f6aef87db；仅本文与advisory_strategy_conditioned_model_blueprint_v1_20260710.md。通过多轮设计审核/F2/当前HEAD CI后按已授权合入及自身官方清理。随后最新main独立源码树事前登记12文件：

- backend/services/advisory_model_first/economic_session_path_v1.py：纯路径/plan及同核薄模型入口。
- backend/services/advisory_model_first/economic_session_path_pipeline_v1.py：零SQL原冻结prepare及薄原子阶段委托。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式session_path_extension、M11真实前驱及47预算；旧默认不改。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M12字段/identity/status路由，旧数学/hash公式保持。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅固定M12/47 fit路由。
- backend/tests/advisory_model_first/test_economic_session_path_v1.py
- backend/tests/advisory_model_first/test_economic_session_path_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- 本文及上述蓝图。

不改旧M1 daily/API/UI、generic input/core、标签/退出/费用、QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS。不提交QE实验、不重建候选/池、不补数/写DB/DDL/DML、不激活数据/profile/模型/配置、不安装依赖、控制服务或他人进程；后端重启user-owned。X临时/F独立新持久。目标架构不是六条并行线，只保留本价格模型主线及M1必要工程辅线。

## 3. Architecture / Contracts / Original source

沿原12D prepared、value-label prepared、386D/7720 KEY/rank/order、包/manifest/池/policy/cost/profile。仅原frozen/raw_daily.parquet的trade_date、instrument、raw_open_cny、raw_close_cny、adj_factor；不用新库、分钟线/当前SQL。M11 evaluated是实际预算及已完成阶段的工程前驱，旧model/收益不用于选新数值；不调用M11重新prepare/fit或旧完整来源函数对当前implementation作资格重验。

纯入口session_path_rows_v1(*, candidates, prices, calendar)，候选仅KEY；价格五列如上。严格原calendar/D-T相邻，先按每个候选20个原session/instrument投影消费对，再校验实际重复与字段；宽源未来/未消费毒化不能影响早D。不压缩历史日期、不删股、不前填/补零。来源D历史声明仍不等同vintage或capture，原等级保留，真实source/hash矛盾报计算错误而非伪UNKNOWN。

输出原KEY/顺序+三字段、session_path_feature_status、session_path_feature_visible_through、session_path_unknown_fields规范JSON原因；不进入模型的状态/原因不得泄漏label成熟度。普通NULL/缺行按依赖字段UNKNOWN，被消费已知数必须真实numeric、非bool/字符串/非有限，价格/AF>0；超大数转换和派生不合法都报ValueError，不clip/winsorize。

## 4. Fixed new information

固定20原session，i=1..19相邻间隔；第0个session的Open不消费，未来T开盘绝不进入D信息。定义o_i=log(O_raw_i)−log(C_raw_(i−1))+log(AF_i)−log(AF_(i−1))，h_i=log(C_raw_i)−log(O_raw_i)。同日AF在h中抵消，不先形成可能溢出的绝对调整价格/比值。共同价格单位/AF缩放不改变含义，数值验证使用明确容差而非阈值选样。

| 字段 | 唯一固定公式 | 实际依赖 |
|---|---|---|
| overnight_log_return_mean19 | sum(o_i)/19，log-return | Close第0～18、Open第1～19、AF全部20 |
| intraday_log_return_mean19 | sum(h_i)/19，log-return | Close第1～19、Open第1～19；不需AF或第0 Close |
| overnight_absolute_log_return_mean19 | sum(abs(o_i))/19，nonnegative log-return | 同o；不是19日收益概率或资金风险仓位 |

不把mean/risk解释为T收益或交易信号；两部分相加是原19步Close log变化，但分解可在相同Close路径下不同。常价/无跳空等已知平线合法0，不造UNKNOWN或填充值。只当某一字段所需的全部真实值已知才计算；第一Close或任一AF缺失仅使隔夜字段UNKNOWN，日内字段仍可保留；D Close缺失反之不抹掉不依赖它的隔夜量。首19 session不完整仍逐股UNKNOWN_20D_HISTORY。块AVAILABLE只三字段已知，否则UNKNOWN并保存逐字段原因；普通停牌缺bar不压缩成另一个19步序列。

candidate仅原12D+本块三字段+g，共16；matched原12D+g，共13，同共同成熟监督重新拟合一次，非拣选旧matched。候选中的宏观/原score与腿条件不被M12移除或宣称跨包泛化，输入通用F1与本研究角色边界分别报告。

## 5. Model / Frozen value and policy

沿GBDT200/lr.05/depth3/min_leaf30/subsample1/maxfeaturesNone/seed20261004、noearlystop；两臂各gross-Y mean/path-min q10，共4 physical fit。只原training_eligible/values_available、原12D+新块有限/合法g/真实label_information_end<=train_end监督，>=100行/20D；validation仅诊断、test禁止fit/校准/选点。不以label、新块或test构建global支持，仍原train12D/已观测g、100bps桶>=30观察/>=5D、2.5～97.5%范围与真实支持洞。

此父实例train2024-07-04～2025-05-30，val06-03～09-30，已消费test10-09～2026-02-02，label截止2026-03-10；不改QE全局窗口。VALUE_REVIEW_5_V1 Top5/Top40/五有效复评、成本buy.95/sell5.95bps一次、T+1/停牌与限价延期保持，不称五交易日独立Y。原JSON/float32树parity、完整合法tick/多个价格段/支持洞、净期望>0/path downside<=800bps保持，信息不直接生成规则买点或订单。

## 6. Registry / Budget / Atomic stages

campaign=advisory_session_path_v1_20261005、model_id=M12、schema=economic_session_path_v1、experiment_id=advsessionpathvalue_+plan_sha前24位。budget_anchor仍原M2同campaign_root，predecessor_manifest_ref role=session_path_predecessor为真实M11 evaluated；旧plan/metadata/终态只读，与source/policy/cost/profile/root一致，实际43+新4 cap47；旧23/27/31/35/39/43默认保持且拒绝M12 journal。显式session_path_extension依赖完整traded_price_extension/M11的四head与原子stage chain，不新目录清零计数。

typed plan/model_copy dump重验；foreign/partial/重复head、不一致前驱与hash拒绝，原子preregister→prepared→trained→evaluated、receipt/source commit/append-only registry和actual fit journal沿既有薄委托。完成stage exact retry只读，不暗中重fit/重query。study_type=MODEL_TRIAL、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY；不建UI/审批平台或旧历史补账。prepare一次0SQL、保存新块/源身份与状态统计，不复制或覆盖旧输入。

实际fit前后QE experiment/custom_evo/multi-alpha三running只读都0；未知/忙只暂停fit，不停止其它开发或控制QE。线程2、四fit<=1800秒、RSS/新增工件<=2GiB，原候选<=7720/原价格<=500000；每候选20窗口、KEY一对一merge，无行膨胀，复杂度O(源行+7720×20)。原18h段至2026-10-05 18:08、原48h至10-06 02:36不重置；长实验30min检查。

## 7. Evaluation / Honest conclusion

原81D/1620候选/100共同估值日完整四臂：baselineTop5、固定±300bps rule、matched、candidate。五固定槽/现金0、不补Top6、原限制延期/费用一次；UNKNOWN研究控制单列且非真实model TAKE/生产回退。保持held mark/未结算/端点检查，不删episode来过条件。

两配对mean日净增量>=原5bps、各>=12原D且>=15%干预、>=30真实model TAKE、MDD相对两者恶化<=200bps、最差5%日均恶化<=20bps。block5/reps2000/seed20261004是描述性区间，仅旧开发窗口导航。负向STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，不换窗口/seed/阈值/特征或选旧控制；不是QE包消费或全局研发门禁。正导航仅考虑独立确认设计，无独立OOS/DSR/PBO、sealed/新holdout/自然前向或激活声称。

## 8. Implementation Plan

三轮设计审核/修订与F2→同步M11 source merge/currentHEAD CI/设计merge/own cleanup→最新main源码树登记12文件→纯数学+薄来源/预算/路由→多轮源码审核/修复、一次稳定最小直接矩阵/Ruff/F2/L0→干净producer/newplan preregister/一次prepare→QE idle一次四fit/四臂→更新真实结果与蓝图/currentHEAD CI/source合入/自己精确官方清理。旧负结果不重跑；当前pending通用label只暂停其新目标，不设置其它资格或全局停止。

## 9. Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-930 | 新Open分解与Close路径信息差；不称T价预测/套利或跨包权重 |
| F-931 | 实际20session/19间隔消费投影、首Open未消费与正常UNKNOWN原人口 |
| F-932 | 三手算/平线合法0/单位复权缩放/有限数/逐字段依赖 |
| F-933 | 同13/16监督、原支持/label/成本/四臂，不消耗test fit/校准 |
| F-934 | 43真实加4/47显式扩展、旧默认/源与完整阶段/partial/exact retry |
| F-935 | QE互斥/原截止/X-F/资源和零公共改动/DB/激活/服务控制 |
| F-936 | 精确范围/多轮审核/直接矩阵、工程/研究/经济/运行分报 |

## 10. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-930 | §1/4/5 | artifact: 新信息与旧三Close字段对照/显式角色边界 | DESIGN_VERIFIED | none |
| F-931 | §3/4/11 | artifact: 真实消费及首Open/普通缺行/不删填合同 | DESIGN_VERIFIED | none |
| F-932 | §4/11 | artifact: 冻结公式及平线/缩放/有限数/字段依赖验收规格 | DESIGN_VERIFIED | none |
| F-933 | §5/7 | artifact: 同监督/原policy及四臂、旧标签不换口径 | DESIGN_VERIFIED | none |
| F-934 | §6/11 | artifact: 43+4/原默认保持/无计数重置与原子合同 | DESIGN_VERIFIED | none |
| F-935 | §2/6/12 | artifact: 原授权模块/时间/资源及QE互斥边界 | DESIGN_VERIFIED | none |
| F-936 | §2/8/11/12 | artifact: 12文件与重复审核/直接测试、证据分层 | DESIGN_VERIFIED | none |

## 11. Direct tests / 最小高价值矩阵

一小fixture覆盖手算/同Close不同Open、o+h恒等、平线0、共同价格/AF缩放、首Open毒化不消费；依赖缺失局部UNKNOWN，源缺原session不压缩，未来/未消费毒化无影响、消费重复/坏值/错误D-T失败。同13/16成熟mask/test毒化无fit效应、JSON数学/原模型身份路由；prepare原顺序/0SQL/atomic retry/tamper，source/predecessor/budget identity与actual43+4、旧默认拒绝M12/partial/QE未知。扩现有budget/identity直接节点，无重复大fixture/实现快照/所有权转移；广回归交当前HEAD CI，不重跑旧收益/20日批量。

## 12. Risks / Rollout / Rollback / Production Gates

真实历史Open提供新观测但未必可学习或有盈利增量；同旧开发数据串行选择仍适应性偏差，不能把单指标改善称确认。原标签依赖原包复评、价格支持非真实分钟fill，缺失原股UNKNOWN而非整项目门。默认无人调用/无新daily family，无API/UI/配置/模型激活，backend_restart_required=false；回滚只停止新M12研究调用，旧数据/工件不动。原goal及截止不重计。

DESIGN-COMPLIANCE-001逐项：设计/known-key检查非功能或模型完成，UNKNOWN不伪成功；原人口/policy/成本不缩减；不新增包资格/平台/公共修改；收益、source merge、cleanup及运行态分开。多轮为本窗口分视角自审，不冒称独立外审；实现/真实研究另回写状态。

## 13. 三视角设计审核与修订

信息/经济轮按真实M7源码核实旧块只消费收盘路径，M12不是price_drawdown或已有D candle位置换名；数学绑定19间隔的o+h等于原Close变化，使用Open分解仍需实测可学习与成本后收益，未承诺预测T开盘。PIT/缺失轮将“20个Open全必需”精确修订为第1～19，首Open未消费；分别列出gap的前19 Close/全20 AF与日内的后19 Close/不需AF，正常缺失只影响依赖字段，不借停牌压缩原session。工程/交付轮明确真实M11 evaluated/43计数及所有旧默认保持，禁止从新目录重计/直接调用旧implementation资格验证；本设计树已ff最新f6aef87db，只有两个文档可写，源码/收益未执行。

F2七项初检PASS；最终F2/diff/精确scope与当前HEAD CI分别核实后才交付设计。此为本窗口三轮自审，不是独立外部评审。原goal继续，实际43+1不冒称拟议47；不为旧失败固化证据或发起资格审核。
