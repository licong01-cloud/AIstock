# Advisory 父预测横截面尺度状态 M21 F2 详细设计

> 日期：2026-10-05；feature tier：F2；docs-fast-new。
> H-PARENT-CROSSSECTION-SCALE-STATE-PRICE-1。仅日频条件买价价值，QE拥有上游Alpha；不研发分钟执行，不重新训练/认证策略包。
> 这是目标主线的下一不同信息假说，不是为了挽救M20而换阈值/seed/loss/窗口；设计、源码、模型、收益与运行配置分别验收。旧48h截至2026-10-06 02:36 Asia/Shanghai，不重启计时。

## 1. Background / Current facts / Business objective

M20源码#5493/currentHEAD9d32fa707/currentCI37319529339 SUCCESS后14:01:38UTC合入c046036f68f9920162443206e7955fd3642036b9，自己official cleanup_done21.468秒/无blocking或warnings/正式F保留。已一次研究：candidate/base/新matched在100共同NAV日26.7299%/21.3220%/15.3803%，配对日+4.46372/+9.54339bps，两个描述性区间跨零，原至少5bps分类未过。只停止自身、NOT_CONFIRMED，不改其结果/标准，不重训、补证或回选旧权重。M1 API/UI六场景和BUG1726公共smoke仍是独立工程缺口，不用本研究冒充完成。

M21检验：同一候选自己的raw及normalized分数之外，父腿当日整个横截面预测的中心/尺度状态能否改善“给定买入价g的净价值与路径风险”估计。候选rank/自己的raw不能唯一确定该日预测分布；增加原冻结其它候选关系的信息，而不是给同一向量换loss/参数。不是上游alpha挖掘，也不把分数尺度直接解释收益概率。

## 2. Scope / Non-goals / Ownership / Authorization

SOURCE限自己的独立latestmain树十叶：economic_parent_scale_state_v1.py、economic_parent_scale_state_pipeline_v1.py；既存economic_sector_price_value_v1.py及economic_sector_price_pipeline_v1.py只新增M21 matched/raw route、status和83预算；四对应直接测试叶；本设计和蓝图。设计交付先仅两docs；SOURCE scope在写前另登记。

不修改M20或旧moneyflow预算链，不修改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS；不重选股、发布因子或增加包资格门。0DB读写/DDL/DML/profile或数据激活/依赖安装/服务及他人进程控制。X临时/F独立正式产物，后端重启user-owned，本研究工具无重启需求。fit前后三QE公开running只读核查，实际Advisory fit不与QE训练并行；回放逻辑不改QE。

## 3. Existing source feasibility / Evidence limit

原冻结parent manifest65b095bf40974f9a4edaed420b5770bbedb94bb6b5a123ff3ec524d224dd25df、原包pkg_ma_8ec5e389fa2c5e484a1ac7e9/manifestf5b008d0...、原feature/labels/profile与M20相同，不刷新父模型或数据。原role LSTM=a1_plus3_LSTM_h20、FUND=new_FUNDGROWTH_h20由冻结角色map取得，列分别raw__/norm__，原normalization=zscore及terminal weights同时绑定。

输入几何spike spec23999b9be8947af4a61c291b8eea97f92eff63a50f7fe1b71d7003424c70c0b0实际只读原D raw/norm、KEY及身份，0.125秒、386D7720原候选、两腿386D可近似确定尺度，最大relative残差2.593341075e-8；0fit/labels/returns/价格/SQL/sealed，不是可学性、原native或收益证据。读取的是金融分数数值，不能谎称仅schema。最初1e-8恒定精度低于实际LSTM raw float32；修订为存储schema dtype的16epsilon，不涉及收益结果或调交易门槛。FUND及两norm存储float64。原NON_VINTAGE/RECOVERED_LIMITED/native UNPROVEN照报。

## 4. Architecture / Mathematical information / Feature construction / Clock

原zscore定义z=(r-mu)/sigma。单候选(r_i,z_i)只给一条等式，多个mu/sigma均可成立；第二不同z候选可增加识别关系。例r_i=1,z_i=1既可对应mu=0/sigma=1，也可对应mu=-1/sigma=2，两情况下该候选M20元组不变而其它原候选raw不同。这里用同D原Top20多行关系近似恢复状态，不读取全市场预测/重新选股，也不宣称恢复了全市场成员或原生归一化metadata。

固定四新量按顺序：parent_lstm_raw_center_D、parent_lstm_raw_scale_D、parent_fund_raw_center_D、parent_fund_raw_scale_D。每D/leg，对原请求KEY中正常finite raw/norm配对，v=sum((z-zmean)^2)，s=sum((r-rmean)*(z-zmean))/v，m=rmean-s*zmean。至少两有效配对且distinct z，否则UNKNOWN_NORMALIZATION_MOMENTS；不能因为选中股票分数相同就假设全市场sigma=0。s必须finite且>0，m finite；最大绝对r-(m+s*z) <=16*max(eps(rawdtype),eps(normdtype))*max(1,maxabs(r))，其中dtype来自原Parquet schema、仅float32/float64合法，不为结果选宽松容差。这是数值计算一致性边界，不是包评分/alpha门。

正常NULL/quiet NaN只退出该配对计算，不删除候选行/日：若剩余有识别关系，可给其余股票m/s；本股票own raw缺失仍UNKNOWN。不足配对保留全日UNKNOWN，不填0或forward-fill；实际bool/string/Inf/sNaN等请求输入错误显式报错。投影请求的原KEY后才消费数值，不消费未请求review/future/其它股票坏数。多次元数据key一对一、原顺序/nextT/包manifest/角色及D截止绑定；D query cut不是伪造捕获时间。

## 5. Contracts / Plan / Model / Supervision / Original policy

ParentScaleStatePlanV1：schema economic_parent_scale_state_v1、campaign advisory_parent_scale_state_v1_20261005、model M21、新advscalestate_身份；原SOURCE_FIELDS/profile/universe、原budget anchor，predecessor ref role parent_scale_state_predecessor指实际M20 evaluated，prior_fit_journal_sha256固定其完成后的80行原字节前缀。新参数/rolemap/score dtype绑定原source。原Top20/原7720/386D日期/Top40 review不换股，不替换输入文件。

matched为12D+own两raw+g共15；candidate同15再四尺度状态共19，两个新训练臂共享正常可用/成熟监督，不能复用M20权重。候选information block是raw2+state4，共六字段；matched recipe只在M21新增own raw两字段，旧M19 sector matched/16-19、M20 13-15、其它旧family/identity原值保持。

GBDT200/lr.05/depth3/minleaf30/subsample1/seed20261004，mean/path_q10各两臂四物理fit，sklearn1.8.0/scipy1.16.3，无earlystop/grid。原train2024-07-04～2025-05-30/val2025-06-03～09-30仅诊断/test2025-10-09～2026-02-02/label cutoff2026-03-10只是本plan字段，不硬编码QE全局。共同成熟训练至少100row/20D仅计算最低量；test不训练/校准/选阈值，价支持仍由原完整train values_available域决定，不按新信息可用/Y符号/行动过滤。原VALUE_REVIEW_5_V1（五有效review≠五交易日）/D终值锚/净值费用公式/退出policy不变。

## 6. Cumulative budget / Source identity / Atomic stages

先实际M20四stage/hash/ledger和真实13/15四heads/4fit/0index闭合；只消费它的完成metadata/hash/必要prepared行，不重审旧failed收益或调用旧sources要求implementation等于新hash。80行原字节前缀含79PHYSICAL_FIT+1INDEX_BUILD，所有原heads/身份唯一/STARTED及旧M4 index绑定；SHA及原root不可替换，M1～M20各4，M3=5/M4=2。只追加本M21四独特head至83，无index，无孤儿/foreign/reset，老11～79cap不变。

typed model_copy重新验证Literal/role/root/映射，clean producer及精确blob先于正式登记/数值prepare；preregister/prepared/trained/evaluated immutable atomic、stage/ledger链/hash与profile/原source/cost/policy绑定。exact已发布prepare只读返回，partial STARTED不得隐式refit。registry沿EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY，不新增公共枚举/UI/审批平台。新私有budget不继续旧500行扫描链；journal<=512KB/source<=20000/candidate<=7720/prices<=500000/RSS2GB/artifact2GB/fit1800秒，元数据循环有限、D group计算线性、精确KEY one_to_one无笛卡尔积。M20产物只读，自己的新正式F不覆盖旧研究。

## 7. Conditional price set / Complete business comparison

输出给定g和D新状态的多段/空/UNKNOWN收益价格集合，不预测T开盘价。两臂共同UNKNOWN、支持洞不连桥，空query验证schema/arm/hash后typed空返回；相对报价误差不是收益验收。真实T开盘仅作为观察买价，不把T收盘/成交/未来收益当D特征；原参考D、CNY/tick、停牌限价信息按既存适配和费用规则。

原81D1620候选100共同NAV日candidate/新matched/SelectionTop5/固定±300bps完整四臂，五槽/空槽现金收益0/Top40 review/T+1/正常停牌与限价延期、held-mark、端点、结算和成本一次保持。UNKNOWN原动作只是研究控制，真实TAKE/UNKNOWN盈亏分账。没有删除日期、把名义日线端点当真实分钟fill或以股级胜率替代组合净收益。

## 8. Evaluation / Evidence boundary / Risks

原paired日net>=5bps分别对baseline/新matched、干预>=12D且>=15%、真TAKE>=30、MDD恶化<=200bps/tail<=20bps仅本研究候选导航分类；5D/2000次/seed20261004两个block区间只描述开发，不能因M20差0.53628bps而更改标准。负向STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，正点最多CONSIDER_CONFIRMATION_DESIGN_ONLY，不停止全部研发或新增QE包资格门，允许直接消费包的用户决定不变。

序列第21假说及已消费窗口选择偏差保留，零sealed/OOS/自然前向/收益确认/activation；几何spike不消除此偏差。风险：raw尺度可主要来自label处理/模型输出尺度噪声而非市场行情；近似恢复不是native metadata，float32舍入与微小norm跨度可能不稳定；新state常数每日可与日期/市场条件过拟合，局部收益正点不代表跨regime稳健。本次不搜索最佳聚合/同族参数/价阈值，不补失败证据或承诺绝对/指数超额收益。

## 9. Production gates / Rollout / Rollback

Rollout只Advisory离线研究工具，不改API/UI/订单/资金仓位/生产descriptor/binding或日常调度。工程完成、模型效果和runtime分报，正式研究可跑不要求重启后端。Rollback仅停止本candidate并保留F正式产物，不回滚QE包或控制服务；任何未来运行接入另行明确角色/family，不把本包专用权重假装通用模型。M1六UI及BUG1726公共smoke两辅线保留，不私改公共runner或绕过验收。

## 10. Implementation plan / Verification plan / Delivery

独立latestmain设计两docs→信息/数值/PIT/人口/对照/预算/边界三轮修订/F2七matrix/currentCI合入→精确十叶SOURCE→同row不同原D横截面的信息反例/配对不足UNKNOWN/normal missing/float32精度/15-19同train与test毒化/原价support/四stage预算/partial/旧M1-M19-M20 bundle多轮修复→稳定最小直接测试/Ruff/scope/F2/L0/clean producer→一次preregister/prepare/fresh三QE空闲4fit/完整四臂/post→真实结果blueprint/currentCI合入/自己的官方清理。不得复跑M20收益、不凑时长或重置48h。source没有实现前不能报此完整链交付。

## 11. Design Acceptance Index

- F-001 新跨候选信息的可识别反例及不冒称alpha/收益概率。
- F-002 原冻结KEY/role/package/D时钟/dtype精度与正常缺失/近似非native。
- F-003 新同监督15/19、原label/support/policy/cost和旧family保持。
- F-004 实际M20/79前缀、仅83/typed/identity/atomic/partial/registry。
- F-005 条件价集合/空输入及完整四臂、UNKNOWN/TAKE分账。
- F-006 原导航分类/串行偏差，零sealed/OOS/activation。
- F-007 精确所有权、设计SOURCE多轮交付/自己的安全清理。

## 12. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/4；planned economic_parent_scale_state_v1.py | artifact: spike23999b9b/跨候选反例；test: planned test_economic_parent_scale_state_v1.py | DESIGN_REVIEW_PASS | none |
| F-002 | §3/4；planned economic_parent_scale_state_pipeline_v1.py | artifact: 原386D7720/float32残差；test: planned test_economic_parent_scale_state_pipeline_v1.py | DESIGN_REVIEW_PASS | none |
| F-003 | §5；planned economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；planned 15/19/old bundle/test毒化 | DESIGN_REVIEW_PASS | none |
| F-004 | §6；planned M21私有预算 | test: backend/tests/advisory_model_first/test_economic_parent_scale_state_pipeline_v1.py；planned actual M20/79/83/typed/partial | DESIGN_REVIEW_PASS | none |
| F-005 | §7；planned nodes/evaluation | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；planned complete四臂/unknown/support | DESIGN_REVIEW_PASS | none |
| F-006 | §8/9；planned registry/evaluation | artifact: 原研究导航/零sealed；test: planned evidence boundary | DESIGN_REVIEW_PASS | none |
| F-007 | §2/10；精确交付 | test: backend/tests/advisory_model_first/test_economic_parent_scale_state_pipeline_v1.py；planned scope/F2/L0/currentCI/自身cleanup | DESIGN_REVIEW_PASS | none |

矩阵只为详细设计审查，M21 SOURCE/preregister/prepare/fit/evaluation均0；当前实际79fit+1index是M20及以前累计，不重复计为M21。

## 13. 三轮设计审核与修订

第一轮信息/经济：单股raw/norm无法唯一决定父横截面中心/尺度，另一原候选增加信息；不混合旧权重、M20正点选阈值或新增父包认证。新matched含raw两量、candidate只加四状态，经济目标仍条件价净值/路径风险，不冒称上游alpha发现。

第二轮精度/缺失/PIT：spike实际读D金融分数但无价格/收益/labels，明确非0数值。存储raw float32使恒定1e-8过严，按schema16epsilon冻结数值算术而不按投资结果宽松；approx proxy非native。相同selected norm不证明全市场sigma0，改为UNKNOWN；正常缺配对保留原人口，有效两不同norm可算状态，但本股own raw未知仍UNKNOWN，不造receipt/捕获时间/全市场成员证明。

第三轮预算/范围/交付：M20真实一次79+1是新lineage前提，绑定精确80原行字节，只追加四至83、老caps不改，不为失败重新扫描旧Y/收益档案。SOURCE仅十叶，原费用/支持/成熟监督/四臂与用户重启权保持。设计PASS不能替代source、研究/经济/运行态验收；M1/BUG工程依赖不假完成，旧48h时限保留。
