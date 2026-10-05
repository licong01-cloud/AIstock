# Advisory 板块与个股资金流联合价格价值 M19 F2详细设计

2026-10-05；DESIGN_REVIEW / SOURCE_NOT_IMPLEMENTED / MODEL_TRIAL / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M18已一次四fit/完整四臂、只停止自身candidate，真实71研究fit+1旧index。M19检验H-SECTOR-FLOW-COHERENCE-PRICE-1：相同股票D状态及板块定价状态下，个股大单资金方向/占比是否改变给定买价的未来净价值。板块扩张不等于该股有边际资金承接；反之资金强也不代表板块环境适合付出溢价。联合信息允许固定GBDT学习这类条件关系，不声称必然有效。

不是把已失败模型预测做集成，也不是按旧窗口收益回选M1或M6权重。原M1/M6各效果不改判，本研究使用已冻结原特征作一个新的信息集合，控制与candidate同新共同监督重新拟合；一次性新假说属于串行自适应开发尝试，计入累计trial/lineage，绝不能冒称独立确认。旧M1的正点估计及两跨零区间不是本研究的效果证据。

## 2. Scope / Non-goals / Feature tier

F2。本设计提交仅本文件与蓝图；实际实施之前登记如下精确12文件：

- backend/services/advisory_model_first/economic_sector_moneyflow_v1.py
- backend/services/advisory_model_first/economic_sector_moneyflow_pipeline_v1.py
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_moneyflow_v1.py
- backend/tests/advisory_model_first/test_economic_sector_moneyflow_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_sector_moneyflow_m19_f2_design_20261005.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS，不重新选股/查行业或读取新行情。无数据库访问/DDL/DML、依赖安装、profile/model activation、API/UI或daily binding及服务/他人进程控制。临时X、正式F独立hash目录；原48h总时限不重置，18h阶段已到点不是业务完成。

## 3. Architecture / Contracts / Existing-input feasibility / Identity / PIT

0trial metadata spike spec SHA256=8f4c58f7619a5a19f66cf581baccc29d850fd04eb91a61b2a11d7a0d3a9003bf，0.016秒，仅Parquet schema及原KEY/status。两源均7720原KEY/386D，sector AVAILABLE3505、moneyflow7700、joint3502/366D、UNKNOWN4218；仅3条sector可用但flow未知。0SQL/数值特征或label/收益数组/fit/sealed。仅证明输入可组合及数学信息可区分，不证明可学习性或盈利，也不是原生身份补证。

固定来源为同R2 root的M1 advsectorvalue_d22f697febf9d36f501e1216/prepared/manifest.json（文件SHA eae5675e1bc1b7175ffd19ef480e614d2e1172709d4e8a7a0e949299be41b1ea）与M6 advmoneyflowvalue_a4e4e4d38d461616edbad7be/prepared/manifest.json（7c26291a3dfbc0ad43360211114c52c5a5ff9a2c2bcf60fcb5ef4e178e369dc2）。原parent_plan、parent_prices、D feature、reusable value_refs与profileSHA56b47410...相同。

只消费既存冻结source：M1基础行及原三个板块字段、M6仅KEY/三个资金字段/status/visible_through。不消费M6第二套基础/label，也不重新计算金额单位、分类或旧来源；实际prepare在干净producer与预登记以后才读数值/labels。KEY=(decision_as_of_trade_date,target_trade_date,instrument)必须完整精确一对一同集合/原顺序，D→原calendar下一T；两visible_through只能<=D，错误身份/重复或不一致键/未来读取拒绝相关计算。 校验这些冻结manifest/原共同refs及实际计算坐标，不要求旧source的implementation hash等于后续新代码，不加载旧训练器或重新认证原native/父processor。

来源仍恢复的NON_VINTAGE/RECOVERED_LIMITED、native UNPROVEN；旧记录只证明D日期边界，历史发布/capture时钟不补造，不声称自然前向或完整全市场原生股票池。原native限制如实披露，不能变成QE策略包使用或整个研发的准入门。

## 4. Frozen information / Missingness / Identifiability

固定sector三量为sector_ret5、sector_vol20、relative_ret5_sector；flow为large_order_imbalance_D、large_order_turnover_share_D、large_order_imbalance_5D。全部原值、单位、窗口不变，不加衍生乘积、调窗口或搜索子集。sector/12D坐标相同而买卖side金额分配不同可使flow不同，因此新信息不由旧sector坐标唯一决定；这是合成可识别性，不是经验正交、可学习或因果证明。

联合状态AVAILABLE仅两原status为AVAILABLE且实际被消费六量有效；正常NULL/float NaN/quiet Decimal NaN或任一原UNKNOWN逐股联合UNKNOWN，不删原股/日期、填零/前填或借当前源重建。零或负的合法收益/资金方向仍合法；bool/string/Inf/signaling NaN等实际坏数拒绝计算。先投影原请求再校验，不消费外股/未来未请求值。保留两源原状态及可见截止，联合状态不是capture receipt。

## 5. Model / Label / Support / Matched contract

matched固定原12D+sector3+g共16；candidate同16+flow3共19。二者同联合AVAILABLE/有限数/原training_eligible/成熟label及价格支持人口，不能拿旧M1训练结果作本次matched或比较不同监督。matched只控制板块信息，candidate检验新增资金块；本M19的16/19合同不是事后放宽旧M1或M6的13/16合同。

GBDT200/.05/depth3/minleaf30/subsample1/seed20261004、mean与q10 path两臂共4物理fit，无early-stop/seed/loss/超参或信息子集网格。沿原train2024-07-04..2025-05-30、validation2025-06-03..2025-09-30、已消费test2025-10-09..2026-02-02、label截止2026-03-10；train only，validation诊断不选点，test不fit/校准。min100成熟train/20D是实际计算能力要求，非父包资格。

原VALUE_REVIEW_5_V1、12D/g及D终值锚/给定价格转换/费用公式、完整train-only价格支持和支持洞不变；不把五有效复评默认为五交易日、不改退出。JSON树导出/推理与sklearn逐头同值，matched/candidate实际矩阵与权重分别16/19。旧family仍默认13/16且recipe/identity/数学不变；新matched信息字段只在M19显式绑定。

## 6. Plan / Registry / Budget / Atomic stages

SectorMoneyflowPlanV1，schema economic_sector_moneyflow_v1、campaign advisory_sector_moneyflow_v1_20261005、model M19、experiment advsectorflow_+planSHA前24。绑定sector_prepared_manifest_ref(role=sector_moneyflow_sector_snapshot)、moneyflow_prepared_manifest_ref(role=sector_moneyflow_flow_snapshot)、实际M18 evaluated predecessor(role=sector_moneyflow_predecessor)、原基础refs/profile/universe/policy/code及16/19 recipe。

先证明真实M18四stage/hash/四heads/ledger及累计71；源码才为该新lineage显式sector_moneyflow_extension增加4至75，须完整asymmetric_risk_extension及前驱链。旧cap23～71/原index1不重置或放宽，typed model_copy亦重新验证，同root身份/预算替换拒绝。新研究study_type=MODEL_TRIAL、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY，已消费开发窗口登记，不混算成独立确认或oracle study。

0SQL prepare只原两rows投影并KEY one_to_one merge，<=7720行，不作笛卡尔扩张；原price/calendar/ref独立读取仍沿500000上界。四阶段immutable/atomic发布、partial STARTED不得隐式重训或伪称exact retry，旧输入/plan/模型不覆盖；干净implementation/producer必须先于预登记和数值读取。拟合前后三公开QE running均空闲，不提交QE实验或控制其任务。

## 7. Inference / Complete business oracle

同原g条件/合法价格tick输出多段/空/UNKNOWN集合，不预测T开盘价；观察的价格是否落在收益集合才决定TAKE/SKIP/UNKNOWN。matched/candidate均按联合输入合同判UNKNOWN，支持洞不连桥、不用测试Y选价。两组实际矩阵的维数、顺序和recipe/hash绑定，不假装M19可加载为M1或任意策略包通用权重。

完整原81D/1620候选/100共同NAV日candidate/matched/Selection Top5/±300bps规则四臂，原Top40 review、5槽/现金收益为0、T+1/停牌/限价递延及费用一次，不能补Top6。UNKNOWN原动作仅研究控制，贡献/episode和真TAKE分别计；正常数据缺失不全局阻断或删日期。所有episode结算/held-mark与端点完整，日线名义端点非真实分钟fill。

## 8. Evaluation / Evidence boundary / API / Production gates

原相对baseline及本新matched的日net>=5bps、干预>=12D且>=15%、真TAKE>=30、MDD恶化<=200bps/tail<=20bps固定为本candidate导航分类；5D block/2000次/seed20261004的两个区间仅描述开发不独立。风险改善/命中率/UNKNOWN控制不能替代收益；不改旧合同、旧candidate或回选控制。

负向只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION；正点估计最多CONSIDER_CONFIRMATION_DESIGN_ONLY，不整体停止、不消费sealed/独立holdout/自然前向或绑定。串行19个模型的选择偏差及低行业支持仍须披露，不能把union视作低相关或正交组合证明。零API/UI/配置/生产/数据库/依赖/进程变化，无新后端重启需求，重启仍user-owned。

## 9. Implementation plan / Verification plan / Risks / Rollout / Rollback

先三轮详细设计/F2/currentCI合入→latestmain独立12叶scope实现→数学/时钟/身份/16-19与旧family/完整业务/所有权多轮自审修复→失败节点回归后稳定最小直接矩阵/Ruff/F2/L0/真实旧M1 bundle只读兼容→干净producer一次预登记/prepare→fresh QE空闲四fit/全四臂及post→真实结果更新/当前CI绿交付和已消费源树自身官方清理。M1六UI/BUG1726公共smoke仍必要工程辅线，未交付不为本研究造资格门，不绕过其验收。

风险：新union仍可能冗余/过拟合、训练有效样本少，4218UNKNOWN控制可能主导组合；旧窗口反复开发、NV和可见时间未原生证明均限制结论。最低监督不满足只阻断本计算，不删填或静默成功。Rollout仅独立研究工具；Rollback停止本candidate并保留正式F，不改旧权重、数据/配置或控制服务。不得为凑时长继续同族变体。

## 10. Design Acceptance Index

以下七个稳定ID以本M19详细设计路径为作用域；其它已合入设计的条款和ID不改。

- F-001 明确联合新信息及数学可识别性，不集成旧失败预测。
- F-002 原KEY/冻结两源/时钟/正常缺失，0SQL与NON_VINTAGE限制。
- F-003 同共同监督16matched/19candidate，原label/support/公式和旧recipe保持。
- F-004 实际M18/71至75，身份/typed clone/atomic/partial与登记。
- F-005 同条件价集与完整四臂，UNKNOWN控制和真TAKE分账。
- F-006 原导航分类/跨轮偏差，零sealed/OOS/收益确认/activation。
- F-007 精确所有权、多轮审核、SOURCE/model/runtime分离及安全清理。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/4；planned economic_sector_moneyflow_v1.py | artifact: spike8f4c58f7/同sector不同flow；test: planned test_economic_sector_moneyflow_v1.py | DESIGN_REVIEW_PASS | none |
| F-002 | §3/4/6；planned economic_sector_moneyflow_pipeline_v1.py | artifact: 原7720KEY/status/同refs；test: planned test_economic_sector_moneyflow_pipeline_v1.py | DESIGN_REVIEW_PASS | none |
| F-003 | §5；planned economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；planned 16/19/同监督/test毒化/旧M1 bundle兼容 | DESIGN_REVIEW_PASS | none |
| F-004 | §6；planned economic_moneyflow_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py；planned M18真实四stage/75/旧cap/typed clone/partial | DESIGN_REVIEW_PASS | none |
| F-005 | §7；planned economic_sector_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；planned 16-19 node/parity/全四臂/Unknown分账 | DESIGN_REVIEW_PASS | none |
| F-006 | §8；planned evaluation/registry | test: planned SOURCE/economic边界；artifact: 原研究导航合同 | DESIGN_REVIEW_PASS | none |
| F-007 | §2/9；范围及交付 | test: planned scope/F2/L0/currentCI/自身cleanup；artifact: 0SQL spike | DESIGN_REVIEW_PASS | none |

矩阵PASS仅完整设计复审，不宣称代码或研究已实现；当前source/preregister/prepare/fit/evaluation均0。实施和经济结果后续独立报告，不能以design PASS假称功能或收益完成。

## 12. 三轮设计审核与当前状态

第一轮经济信息与可识别性：来源不是同族换loss/window，M1 sector状态与M6订单side分配可不同；不混合旧权重或把M1旧正点估计当确认。只冻结一个union和一个matched，不搜索最佳组合；spike无金融值/Y数组。

第二轮对照/时钟/标签：两源相同原基础身份、7720KEY一对一，M6只取自己的三量和状态，标签始终原M1基础行；16/19共享新共同监督不借旧matched，所有旧family/原五有效review/价格支持与D终值锚保持，UNKNOWN不回填或丢日期。

第三轮数值/预算/交付：原source状态与正常缺失/坏数分开，AVAILABLE3502不是可学或原生PASS；必须真实M18 complete/71后才显式75、四heads一次、原sealed不读、12文件边界及用户重启权不变。完整设计可交付但source/研究尚未开始；详细实现继续逐条核对F-001～007。
