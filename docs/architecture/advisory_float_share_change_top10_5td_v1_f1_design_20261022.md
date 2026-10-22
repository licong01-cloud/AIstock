# 滞后流通比例变化 Top10 固定五交易日研究（F1）

版本v1.0；2026-10-22；Advisory / model_first。
状态：输入检查完成，设计待交付；尚无本候选监督fit、评价或经济确认。用户本轮选择“新信息来源：查重与输入验证后研究”；P4/P5已结束，本研究不改其结论或自动进入确认。

## Background / Goal / Feature Card

唯一假设 `FLOAT_SHARE_FRACTION_CHANGE_5D_TOP10_5TD_1`：在原冻结两包Top50内，最近已发布的流通股占总股本比例变化，可能包含价格/换手/当期规模之外的供给状态，对T开盘至T+4收盘的Top10成本后排序有增量。供给状态变化可能伴随再定价；效应是否延续到T开盘以后、方向为何，均交给本次固定检验，不要求先完成因果识别，也不承诺负系数。

这不是解禁事件日历、实际持有人减持、机构卖压或新抓取的数据集。日股本可能包含公司行动、口径变化或历史修订；公开解禁研究只能支持供给问题的一般动机，不能证明本代理在A股五日有效。参考[Ofek/Richardson原始摘要](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=207908)。不恢复已暂缓的公告，也不请求上游采集。

[M13](advisory_free_float_turnover_m13_f2_design_20261005.md)使用20日自由换手均值/波动与D自由流通规模，不消费历史股本比例变化。本次查验两包50份冻结动态源码全部hash一致，没有直接引用db_float_share/db_total_share；唯一市值邻接因子dynamic_valuation_factor是估值倒数之和除总市值。两包另有20个Alpha158量价字段，未据此证明与完整父模型统计独立；换手与量价可能间接编码股本，父序列模型也可能已学习相关信息。新增的是一个明确的历史股本状态，不是从父包准入或旧阴性推导它必有效。

## Input / 来源、时钟、单位与UNKNOWN

仅父研究 `advmultitop10_6c632adf532818cfcd059113`：
`X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/advmultitop10_6c632adf532818cfcd059113`。
plan文件SHA `49c3d4a525299ae89a9702f8eb717a579ba96a0e2181ba441c09a4d5c8b88f61`；binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。
两个包pkg_cc7eccb6202e4816b9d7c7d6b748bd37、pkg_8dbd396d27184de6aa5fe288011220f6仅用于来源/分组，不作学习字段或生产白名单。原人口305D/30,500行，D=2024-07-04..2025-09-30全部保留。

源固定为该binding的daily_basic.h5两列db_total_share/db_float_share；两者万股、float32。来源映射以backend/qlib_exporter/db_reader.py::load_daily_basic_panel及[Tushare官方说明](https://tushare.pro/document/2?doc_id=32)核对。官方更新15–17时，故特征在D只取D-1及D-6两个原交易日端点；不是D收盘瞬时取D盘后数。历史文件仍CURRENT_DATABASE_NON_VINTAGE，滞后一日不创造captured-at证明。

设S为总股本、F为流通股本，a=D-6、b=D-1：

| 输入 | 固定定义 |
|---|---|
| ff_fraction_lag1 | F_b/S_b |
| ff_log_total_shares_lag1 | log(S_b)+log(10000)，实际股数自然对数 |
| ff_log_total_growth5 | log(S_b)-log(S_a) |
| ff_fraction_delta5_bps（唯一新增量） | 10000×(F_b/S_b−F_a/S_a)，比例基点，不是收益bps |

前3列为共同控制；matched=原八字段+3列，candidate=同11列+q，共12列。相同当期比例、总规模和总股本增长仍可有不同q。每端点同时倍增S/F不改变q，但它不能消除所有公司行动/历史修订影响。

任一必需端点/字段缺失保持UNKNOWN及原槽；零变化是合法0，不借缺失填零。已知非数、bool、inf、S<=0、F<=0、F>S、重复/外来键为错误，不clip。前6日历史不足明确错误/未知，不压缩停牌日或另选端点。只投影该D所需两端点再数值验证，未来/无关坏数不能影响早D。完整研究按声明日期范围/原成员集合一次有界读取两列，最大4M数值单元；每D再次严格投影。无SQL、active重解析、大文件复制。

## Feasibility / 事前无标签检查已完成

input_spec SHA `bdb93c32a349f96f042f8634278c11f2a28730ca129b106cf322fd45346666ae`；
screen receipt `X:/AIstock_temp/ADVISORY/p6-float-share-change-20261022/screen_receipt.json`，
SHA `435e2b1c4928139e26998c6abb88518dd5c6339fe10389789e703a0deaafe571`。
该收据及spec、结果行hash须绑定本轮预登记。daily_basic全文件SHA已复验为`3962873854ae212278d7e2020eb1401c5f64589e882c6124ff29bf16e0e7388f`；随后使用相同pin/尺寸/stamp及实际消费片段hash，不能把stat冒称重算全文件hash。

第一训练窗179D/17,900原行/16,280唯一stock-D完整保留，读取65,034源数值单元；16.9414秒，峰值234,479,616bytes。TCN/LSTM可算8927/8932（99.7430%/99.7989%）；缺端点23/18原行保留。达到绝对1比例基点的原行168/144、日期96/86、股票72/50；三个事前块分别113/39/16及84/38/22行。两包均满足事前80%可算、20D、20唯一stock-D、至少两个块的输入支持条件。

1bp仅为本次事前样本支持量级，模型使用全部连续q，不截断、不按该阈值交易。精确非零248/242行，但float32全邻接间距敏感性可区别于零的只有212/213行；这不是统计区间。q大多数为0，IQR为0；此事实在读标签前决定下述固定尺度处理，不按结果择变换。
单位/同比例拆股不变、当前比例相同而q不同、缺失/常数/坏值拒绝、未来投影及浮点量化界检查通过。0标签/0fit；这些不证明整个305D成熟监督支持或经济价值。

## Model / 唯一有限实验

三折沿原FOLDS：train_end=2025-03-31/05-30/07-31，验证2025-04-01..05-30、06-01..07-31、08-01..09-30。训练只用D<=cutoff且label_information_end<=cutoff的成熟AVAILABLE/合法CASH_ENTRY_NOT_EXECUTABLE，现金目标须真实0。两臂相同共同支持；不删原推理人口。

固定Ridge(alpha100, lsqr, tol1e-6,max_iter2000,fit_intercept=False)。各折/各臂训练内逐列median；IQR>0用IQR，否则非恒定列用该列有限训练值的population std，恒定列scale=1；全部训练缺失列停用。两臂共同列具有完全一致编码统计；编码后同包/D中心化X和Y，D等权、D内包等权、包内监督行等权。每列另有missing bit。此尺度处理对稀疏非恒定q保持单位可比，不增加变换搜索；标准差是编码统计，不是额外监督模型。禁止旧模块全局替换或修改共享标准化。

原基础八字段允许显式训练中位数编码，三个股本控制和q必须同支持可知；没有任何基础字段可知则NO_RANK_ADVICE。各组只对可建议槽排序、平手沿原rank，未知固定原槽；原Top50/分数/身份不变，选前10。状态/未来标签不可用于推理预筛选。

恰好最多3折×2臂=6真实监督fit，无参数/模型/窗口/符号/包搜索，不继承P4额度；prior213、成功本轮累计219fit＋1旧index。fit前独占claim，已STARTED未发布的fit明确中断，不自动重fit。每折两臂预测先原子冻结，再解码该折成熟评价金融值；两臂训练KEY hash相同。完整研究先bind清洁源码commit/实现闭包/环境/父stage/源pins及本screen证据。

## Evaluation / 终点、停止与证据

主终点candidate−原生Top10，沿原T开盘至T+4收盘固定十槽净cohort、买0.95/sell5.95bps各一次。合法不可入场现金0，真缺价/未成熟不冒充0或补下一名。matched只解释新增股本变化；不回选matched或择包。固定126原评价D，按三臂两包共同完整日配对，先包等权再D等权；缺失原日期/行分报，不将股票行或跨包同股当独立样本。

复制当前原生inner分类：主均值>0、至少2/3折正且每折可评价、共同成熟D覆盖>=80%、原生干预>=20D且>=20%成熟D并跨两折、尾部恶化<=100bps、额外UNKNOWN<=2pp，才INNER_SUPPORTED；均值<=0或超风险为INNER_NOT_SUPPORTED，其余INNER_INCONCLUSIVE。95%描述区间跨零不追加否决，matched/系数不追加门槛。同步10D circular block/10000次/bootstrap seed20261021，完整报告区间/MDE、两包/三折、干预、UNKNOWN、两臂控制差与新增系数。

所有终态都结束本次候选。即使SUPPORTED也仅支持讨论后续确认，**本研究没有final/outer/sealed路径或自动第七fit**。不读旧outer标签，不因新目录恢复独立性。已消费开发、NON_VINTAGE、选择偏差、重叠5TD均保留；不是可执行成交、NAV、独立OOS或盈利确认。P4 INNER_SUPPORTED、P5当前不推进确认保持原结论。

## Scope / Implementation Plan

设计独立PR从main `17f16f353ae9054709f0263478dff1c868ef9274`交付，只写本文；设计合入后从最新main创建源码树。精确六文件：

- backend/services/advisory_model_first/float_share_change_top10_5td_inputs_v1.py
- backend/services/advisory_model_first/float_share_change_top10_5td_v1.py
- backend/services/advisory_model_first/float_share_change_top10_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_float_share_change_top10_5td_v1.py
- 本文
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

复用真实接口：frozen_dataset::inside/stamp；multiperiod_top10_5td_inputs_v1::IDENTITY/INPUT_FEATURES；peer_residual_top10_5td_v1::validate_population/group_center/panel；peer_lead_lag_top10_5td_v1::summarize；economic_entry_pipeline::publish_stage/read_stage；prequential_top10_5td_study_v1::projected_finance；research_control的registry/独占lock。以flow_price_coupling三叶的明确已验证模式复制成本次本地合同，旧源与全局变量不改，不运行旧研究。

source.finance只读父prepared/train_finance，先投影KEY/H，再带H<=cutoff解码，拒绝outer或任何end/cutoff>2025-09-30。父evaluated stage仅核byte/hash身份，不解码其收益。输入先准备、与事前17,900行q逐值对齐后才真实fit；此检查集成prepare而非另跑研究。

## Operations / Test Plan / Verification Plan

2CPU线程、无GPU、RSS<=2GiB、阶段<=30分钟、输出<=200MiB；新fit前只读QE公开状态和资源，资源足够可并行，不控制其它任务。输出全X:/AIstock_temp/ADVISORY/p6-float-share-change-20261022/；旧输入/stage/registry不覆盖。新registry明确本次1候选、6fit而非独立发现。

直接合同覆盖手算/单位/同比例拆股、D-1/D-6投影及未来毒化、UNKNOWN/非法数、源重复与预算/漂移、稀疏非恒定std回退及单位不变、共同训练支持/成熟H、预测冻结、固定槽/Top10、6claims/拒绝final与outer、幂等不重fit。先定向失效点再稳定后一次相关小矩阵，Ruff/L0/ownership/diff/F1及蓝图F2必需CI；无跨模块全量重测。两轮同窗口自审，不冒称独立外审。研究完成回写真实结果并按既有授权提交/合入及自身官方清理。

## Design Acceptance Index

| ID | 验收目标 |
|---|---|
| F-481 | 新信息语义、已消费证据边界与冻结screen身份 |
| F-482 | 两端点/单位/float32/缺失及原人口完整 |
| F-483 | 共同成熟训练、固定稀疏尺度/三折六fit |
| F-484 | 原生产品终点、预测屏障与无outer |
| F-485 | 原子身份/claims/恢复及预算 |
| F-486 | 真实研究完整报告与模块/生产边界 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-481 | 本文Background/Feasibility | artifact: X:/AIstock_temp/ADVISORY/p6-float-share-change-20261022/screen_receipt.json；factor_audit.json | DESIGN_READY | none |
| F-482 | inputs叶计划：measure/read_panel/attach | 计划：backend/tests/advisory_model_first/test_float_share_change_top10_5td_v1.py；本文Input | DESIGN_READY | none |
| F-483 | core叶计划：training/fit/predict | 计划：backend/tests/advisory_model_first/test_float_share_change_top10_5td_v1.py；本文Model | DESIGN_READY | none |
| F-484 | pipeline.finance/fit_pair、core.summarize计划 | 计划：backend/tests/advisory_model_first/test_float_share_change_top10_5td_v1.py；本文Evaluation | DESIGN_READY | none |
| F-485 | pipeline.Study/preregister/run计划 | 计划：backend/tests/advisory_model_first/test_float_share_change_top10_5td_v1.py；本文Operations | DESIGN_READY | none |
| F-486 | 本文Scope/真实结果计划 | 计划artifact: X:/AIstock_temp/ADVISORY/p6-float-share-change-20261022/validation-receipt.json；本文Test Plan | DESIGN_READY | none |

本矩阵验收完整设计；源码和真实研究是下一交付阶段，当前未宣称实现通过。第一轮自审校正稀疏字段缩放、D-1发布时间和M13差别；第二轮核对完整人口、输入支持不当经济确认、六fit/无outer、正常UNKNOWN和模块边界。F1格式校验发现non_goals标题及计划证据路径不完整，已按实际设计状态修正，不绕过未实现状态。

## Non-goals / Risks / Production Gates / DESIGN-COMPLIANCE-001

稀疏变化、极端比例变化、历史修订、供给代理含义不唯一、父包间接编码与重复开发消费都可能使候选无用；如实报告，不事后缩放截尾/择股/扩窗/改系数符号。输入支持不是盈利门，不另设父包准入。

设计交付仅为完整设计；源码/真实研究尚未完成，不称模型可用。错误/UNKNOWN不隐去；固定业务/成本/人口不变；无新增设计外审批或门禁。本轮只Advisory离线，不改QE/HMM/LocalSIM/Selection/StrategyPackage/数据发布/公共workflow；无DB、DDL/DML、激活、依赖安装、后端/worker/WSL API/QE进程控制、生产回放。公告、流水线及启动脚本任务不恢复；backend_restart_required=false。
