# Advisory 历史非对称风险条件买价 M18 F2详细设计

2026-10-05；SOURCE_MERGED_CLEANED / STUDY_COMPLETED_NOT_CONFIRMED / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M17已一次四fit/完整四臂，baseline日增量−4.3050bps且区间跨零，停止自身，不救活/重训或改门槛；真实67研究fit+1旧index。M18检验H-ASYMMETRIC-HISTORICAL-RISK-PRICE-1：同历史终点涨幅与趋势几何，不同上涨/下跌幅度及连续下跌能量是否改变给定买价的后续净价值。不是QE alpha生成、重选股票或分钟执行；不声称历史风险必然预测未来收益。

工程spike e2858862...仅核对原冻结prices Parquet380828行及四列metadata，0真实行情/收益数组、DB/fit。合成19个logreturn：A=.01×19；B前五=.002×4+.042、后十四=.01×14。总logreturn、末14路径及单调几何相同，但upside RMS=.01/.0129371；原ret1/5/10、同末14 high/low的ATR14、M7 drawdown/efficiency/up-day-share不能识别该幅度分布。此仅数学可识别性，不是数据实证或经济证据；不会扩宽旧窗口/换seed或loss来复试。

## 2. Scope / Non-goals

设计仅本文件/蓝图；实现先登记显式12文件：

- backend/services/advisory_model_first/economic_asymmetric_risk_v1.py
- backend/services/advisory_model_first/economic_asymmetric_risk_pipeline_v1.py
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_asymmetric_risk_v1.py
- backend/tests/advisory_model_first/test_economic_asymmetric_risk_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_asymmetric_risk_m18_f2_design_20261005.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS，不访问DB/DDL/DML、安装、数据/模型激活或服务与他人进程控制。研究工具不接daily/API/UI或绑定；原名单与日期/政策/成本/标签保留。临时X、正式F独立、原18h/48h期限不重置；正常缺数据不阻断其它研发。

## 3. Architecture / Contracts / Clock

复用原parent_prepared_manifest_ref中的prices.parquet及calendar.json、原reusable的12D+g+labels。原KEY=(decision_as_of_trade_date,target_trade_date,instrument)，D到原calendar下一T，20session≤D的19相邻收益；同已采用的20session窗口，不搜索window。仅trade_date/instrument/raw_close_cny/adj_factor；按原候选实际请求先投影，再校验金额/因子，外股/未来宽源不消费。

只有D可见历史复权因子×价格，先log(raw_close)+log(adj_factor)，再作差，不直接乘积，避免合法极大或极小数溢出/下溢。原源是已冻结恢复的NON_VINTAGE/RECOVERED_LIMITED、native UNPROVEN，不能补capture时间/原生receipt或升级为自然前向。坏实际数/重复源键/错误D-T仅拒绝相关计算，不重新认证QE包。原7720候选、1620评估原股及日期顺序不变，单股缺任一20点仍UNKNOWN、不填删/不前向填充。

## 4. Frozen mathematics / normal missingness

设r_i=log(C_i A_i)−log(C_(i−1) A_(i−1))，i=1..19；用两log相加等价实现。固定：

- downside_log_return_rms19=sqrt(mean(min(r_i,0)^2))；
- upside_log_return_rms19=sqrt(mean(max(r_i,0)^2))；
- downside_adjacent_log_return_energy18=sqrt(mean(min(r_i,0) min(r_(i−1),0)))，18相邻对。

均为daily log-return fraction，不年化、无裁剪/调参。分母固定19/18而不是仅跌/涨天数；零收益合法，全部上涨时downside两量为已知0，平坦时三量为0，不把0伪造成缺失。RMS与相邻量先按max|r|缩放再平方/乘积求均值，避免极值中间运算；乘回scale前检查有限。

原close/factor必须有限正数：NULL/NaN/quiet Decimal NaN正常UNKNOWN，signaling NaN/bool/string/Inf/零/负/非零转float下溢拒绝实际计算。不足20历史UNKNOWN_20D_HISTORY；缺日或正常NULL为UNKNOWN_PRICE_SOURCE。无需行情价/收益Y/label成熟或model action决定来源人口；不读取T open来生成D特征，不用监督值反填价格。

## 5. Model / label / support

matched=同共同成熟样本原12D+g共13，candidate=同13+三风险量共16；GBDT200/.05/depth3/minleaf30/seed20261004，mean与q10路径两臂共4物理fit。train only、validation仅诊断，test不fit/校准/选点；原VALUE_REVIEW_5_V1、支持洞、D终值锚及给定买价转换、成本不改。合成识别不能作为可学习或盈利证明。

## 6. Study / identity / budget

AsymmetricRiskPlanV1/schema economic_asymmetric_risk_v1/campaign advisory_asymmetric_risk_v1_20261005/model M18/experiment advasymrisk_+planSHA前24。predecessor_manifest_ref(role=asymmetric_risk_predecessor)必须真实M17 evaluated、原四heads/全stage/hash/ledger，实际67，再显式4至71；原23～67及旧index不清零、不放宽。

新asymmetric_risk_extension须完整flow_path_extension和所有前驱；typed model_copy也重验证，假前驱/身份冲突或同root预算替换拒绝。source manifest/原policy/profile/universe/implementation进入身份；0SQL prepare一对一KEY merge保序、immutable/atomic发布与exact retry，不覆盖旧输入/研究或读取收益后换指针。源行≤500000、原候选≤7720，固定20点定长计算，不做全市场笛卡尔join。

## 7. Inference / business oracle

同13/16核按给定g计算mean/path及合法tick下多段/空/UNKNOWN价格集合，原support洞不连桥。TAKE/SKIP/UNKNOWN区分，UNKNOWN控制与真模型TAKE分账；完整原81D/1620候选/100共同NAV日四臂，5槽/现金0、不用Top6补买、T+1/停牌/限价递延与费用各一次；所有episode结算，名义日线endpoint不是分钟fill证据。

## 8. Evaluation / API / UI / Production gates

已消费development/NAVIGATION_ONLY，报告成本后共同NAV/配对日增量/固定5D block2000 bootstrap、MDD/tail/干预D/真TAKE。沿原5bps相对baseline及matched、12D/15%干预、30真TAKE、MDD200bps/tail20bps仅分类本candidate；失败只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，不提高胜率便冒充收益，也不取消原条件。0新sealed/独立OOS/经济确认/自然前向/activation，正结果仅导航、负结果不关闭全方向。

## 9. Implementation plan / Verification plan / Risks / Rollout / Rollback

设计三轮审核/F2/currentCI后合入/自己官方清理→latestmain独立12叶源码及多轮自审修复→最小直接矩阵/Ruff/F2/L0/旧冻结bundle兼容→干净producer后一次预登记/0SQL prepare→fresh QE三running0时4fit/完整四臂及post核对→实际结果更新蓝图/设计/currentCI后SOURCE交付与自己精确清理。允许工程先行但必须保留冻结代码闭包至一次研究消费；QE忙只暂停fit，既有18h/48h终点不重计。

风险：历史非对称风险可能只复刻动量/波动已知效应；20点噪声、NON_VINTAGE、已消费开发窗口与跨模型尝试的选择偏差仍在。Rollback仅停止本candidate/保留原study，不修改数据库/配置/绑定或控制运行服务。Rollout仅研究工具SOURCE，无daily/API/UI/自然采集变化。Production gates=DB/依赖/profile/model activation/进程控制NOOP，无后端重启需求，重启权限仍用户。

## 10. Design Acceptance Index

- F-990 原历史非对称幅度/相邻能量与可识别性。
- F-991 原20session/D-T/PIT、先投影、quiet缺值/0/坏数/empty。
- F-992 原prices/calendar冻结身份、0SQL/atomic/保序。
- F-993 共同13/16/train-only/旧公式及冻结bundle兼容。
- F-994 真实M17 predecessor/67至71、旧cap及身份不变。
- F-995 原完整四臂/真TAKE与UNKNOWN/证据分层。
- F-996 精确所有权、自审和交付/正式产物保留。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-990 | §1/4；economic_asymmetric_risk_v1.py | test: test_economic_asymmetric_risk_v1.py；artifact: 同终点/末14与旧M7几何但RMS不同 | PASS | none |
| F-991 | §3/4；economic_asymmetric_risk_v1.py | test: test_economic_asymmetric_risk_v1.py；artifact: 0/quiet NaN/normal missing/future/empty | PASS | none |
| F-992 | §3/6；economic_asymmetric_risk_pipeline_v1.py | test: test_economic_asymmetric_risk_pipeline_v1.py；artifact: 0SQL/hash/atomic/KEY | PASS | none |
| F-993 | §5/7；economic_sector_price_value_v1.py | test: test_economic_sector_price_value_v1.py；artifact: 共同成熟/test毒化/原bundle | PASS | none |
| F-994 | §6；economic_moneyflow_price_pipeline_v1.py | test: test_economic_moneyflow_price_pipeline_v1.py；artifact: 真实四heads/M17 stage/71/旧caps | PASS | none |
| F-995 | §7/8；economic_sector_price_pipeline_v1.py | test: test_economic_sector_price_pipeline_v1.py；artifact: 四臂/控制分账/非激活 | PASS | none |
| F-996 | §2/9；范围/交付 | test: test_economic_asymmetric_risk_pipeline_v1.py；artifact: scope/F2/L0/currentCI/自己cleanup | PASS | none |

## 12. Current state / review

M18源码#5487 HEADb71f0aef72798b4814f49b123d882283a5c4e65a/currentCI37286496082 attempt2 SUCCESS已于09:17:32UTC合入13b4c4232da82d9d0abc1cc0c5f20251da92d23b。实际fit闭包仍为clean producer397aaa83f624a2670cef2642adf89f1635d51a96/implementation a5022095...，不是用后续文档HEAD重训。run advasymrisk_d7d0e9676f7177bb1fbc2cc8/plan d7d0e9676f7177bb1fbc2cc8c0c52ac056c780c864060c23725a45b6f86d17f4原一次0SQL prepare8.594秒/7720原键保留，7330AVAILABLE/380预热/10正常缺源UNKNOWN；等待QE期间未重prepare。10:51:03UTC三running全部0后仅从该冻结闭包一次四fit/完整四臂29.719秒，10:54:52UTC拟合后仍全部0；共同成熟train3693/195D、1591validation仅诊断。原81D/1620候选/100共同NAV日candidate/baseline/matched/rule成本后名义净收益9.4435%/21.3220%/4.5719%/20.5747%；paired日baseline-10.5233bps CI[−34.003257,11.675848]、matched+4.3360bps CI[−11.244744,19.361192]。76真模型TAKE/5UNKNOWN研究控制、81episodes全部settled，只有net_increment不通过，MDD/tail/干预/真TAKE通过；回撤改善不能替代原收益目标，只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION/NOT_CONFIRMED。真实累计71研究fit+1旧index，0独立确认/sealed/真实fill/activation/DB访问写入/QE源码修改或服务控制。冻结源码闭包已实际消费后自己的官方cleanup_done/16.797秒、blocking/warnings空；正式F模型/输入/结果保留。M1六UI与BUG1726公共smoke两未合入树继续保留，原18h阶段18:08已到点而原48h总时限不重置；不重跑M18/回选matched/调门槛或为旧负候选补证。 设计#5486已合入e4c9f7771/自身官方清理；原spike仍仅metadata/合成，后续prepare与一次研究另列真实发生阶段。

第一轮核对M7/M8/M11/M12现有特征与原D信息：不是把ATR改窗口，合成同总回报/末14/单调几何对照识别平方幅度差异；原parent frozen metadata即可，不做新SQL/QE实验或收益预筛。

第二轮逐项核对quiet正常缺值、全平坦/单侧合法0、19/18固定分母、log坐标避免价格乘积溢出、scaled平方/乘积、真正请求先投影和原D/T。不把一切价格缺失判停牌，也不丢原股/日期；不创造known_from/capture或升级原生身份。

第三轮按F-990～996核对M17真实67/全部stage才显式71、旧政策/价值标签/支持不改、12文件边界、训练互斥/反复审核/源码及模型有效性分离。设计矩阵DESIGN_REVIEW_PASS仅此设计复审；所有研究必须后续一次预登记，禁止一批负模型就停止全任务或为时长复试旧模型。

## 13. 实施和准备阶段三轮审核记录（研究前）

第一轮核对F-990～992：同原20session/19-18分母、完整平坦/单侧0、quiet正常缺值、极大/极小价格两log坐标、先真实请求投影和原D/T/唯一KEY。数学对照及19个新叶项首轮18PASS，prepare收据误用旧flow_source_columns被测试发现，已改为本plan stock_source_columns；研究前字段引用和错误消息亦核对，不覆盖旧输入。

第二轮核对F-993/994：真实M17全四stage/hash/ledger/四heads、typed model_copy/同source-policy-profile、累计67至71及旧caps；共同13/16训练、test毒化不改模型身份、原支持与给定买价公式保持，原实际M1 JSON权重只读加载/零fit兼容通过。

第三轮F-990～996/所有权/交付审计：稳定56直接项、Ruff/F2七项七行零warning、feature L0零finding、standard L0三P2复杂度提示/无blocking；候选上界7720/来源500000、固定20点、KEY one-to-one merge无笛卡尔扩张，diff/scope通过。错误消息统一后仅两pipeline项复验/Ruff通过。干净producer397aaa83后一次预登记与0SQL prepare，实际7720原键全部保留；当时QE running1，因此准备阶段尚未拟合/评估；后续实际一次研究见§12/14。矩阵PASS是研究工具源码验收，不宣称研究完成/经济有效；当时冻结实施闭包保留至一次正式研究，SOURCE交付与后续实际研究分别报告，本次不接daily/运行配置或后端重启。

## 14. 实际一次研究与结果审计

原81D/1620候选及100共同NAV日全部四臂结算，不缩窗、删股或回选控制。evaluated/evaluation.json SHA256=f6c4848bd4ce91ca564befa409eaee8f1b932f8e0b3dce0c01870ed780afd06e；evaluated/manifest.json文件SHA256=e19d6a393d9b8ad22fb73df66dabfe44689c526fd58d1275d9df2fd50b0dd9b7。固定bootstrap为5D block/2000次，两个配对区间跨零，仅导航而非独立OOS或激活证据。

三轮结果/交付复审：①F-990～994按原plan/implementation、全部7720键与真实71 ledger核对，未改数学、价格支持、政策或标签；②F-995按完整四臂、76真TAKE与5UNKNOWN控制分账、零未结算/非真实fill核对，不能把9.4435%或回撤改善称为增量有效；③F-996与DESIGN-COMPLIANCE-001四项逐条核对，只更新本设计/蓝图、自己官方清理已消费源码树，正式F保留，UI/public smoke未完成如实留gap，无新增资格门、数据库/配置/进程操作。矩阵PASS是原研究工具及按计划完成研究的验收，不是模型经济PASS；NOT_CONFIRMED/非部署状态不变。
