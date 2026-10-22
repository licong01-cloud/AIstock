# 分钟级留一同群同步结构 Top10 固定五交易日研究（F1）

版本v1.1；2026-10-23；Advisory / model_first。设计#6610合入0a4862420f61a58e27765db45de0336e23248e04并官方清理。无标签查重及输入检查通过；源码15项直接合同测试、Ruff、两L0/ownership通过，真实六fit研究待执行。累计219实际研究fit＋1旧index，不是219个独立假设。

## Background / Goal / Feature Card

唯一假设`P9_INTRADAY_PEER_SYNCHRONY_TOP10_5TD_1`：原Top50内，个股与其余候选在同一分钟的共同价格变化，可能提供单股分钟路径摘要、序列统计和同群边际量以外的排序信息。只检验T开盘至T+4收盘Top10成本后净cohort增量；正负方向不预设，不声称投资者身份、资金流或可交易的跨冲击。

旧分钟研究使用单股汇总/15分钟分箱/序列相关；E2同群残差用历史日频beta，先导滞后用滞后日频关系；它们不直接测同D的跨股票分钟对齐依赖。构造检查保持43个旧单股分钟字段、日OHLCV/日收益及目标股6个共同边际量相同，同步相关从1变为−0.000468，证明表示不完全相同，不证明真实Alpha或父模型独立性。父因子50份源码pin一致，未见直接分钟读取；词法查验不证明完整统计独立。

[Min/Maglaras/Moallemi原始论文](https://arxiv.org/abs/1811.05524)说明美股分钟量的跨股依赖及执行影响，仅支持研究共同结构的动机，不能证明此价格代理在A股5TD有效。继续暂缓研报/report_rc、公告、流水线和启动脚本；数据窗口结束也不自动恢复。

## Input / 来源、时钟、单位与UNKNOWN

只消费父研究`advmultitop10_6c632adf532818cfcd059113`的原冻结文件：
`X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/advmultitop10_6c632adf532818cfcd059113`。
binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`；两包pkg_cc7eccb6202e4816b9d7c7d6b748bd37、pkg_8dbd396d27184de6aa5fe288011220f6仅是来源/分组。研究人口2024-07-04..2025-09-30、305D/30,500原行全部保留。

源为binding固定minute_calendar/minute_meta_export及OHLCV .1min.bin；读原D切片，每唯一stock-D一次，不读T后分钟。原交易日历校验D到T为下一交易日。仅9:30–11:30、13:00–15:00、无时区且分钟对齐的原槽；每D<=300槽，严格递增/唯一，不跨午休、缺失分钟或日边界连接。只将相邻1分钟且两端已知正收盘价的log差作为边。已知inf/非正价格、重复/非法时钟、租约或元数据漂移报错，不clip修复；正常缺文件/缺价保留NaN。

每边对目标股i，p为**原Top50其余股票**可观测收益等权平均，显式减去自身；至少40个可观测其它成员。不是行业/全市场或历史固定同群。只取目标及p共同可观测的边，覆盖须达到该D原合法相邻边的80%。零自身或同群方差保持UNKNOWN。

| 字段 | 定义 |
|---|---|
| sync_correlation | 共同边上corr(个股log收益, 留一同群均值)，无量纲 |
| sync_directional_participation | 上述相关×共同边个股log收益之和，无量纲 |
| 六个共同控制 | 共同边自身/同群log收益之和×10000，自身/同群sqrt(sum(r²))×10000，共同覆盖、最少同群人数/49 |

matched共57列=原8日频/native字段＋39旧单股分钟字段＋4序列字段（serial_correlation、serial_closing_pressure、serial_coverage、nonzero_return_fraction）＋6控制；candidate只新增上述2列，共59。附各列missing bit；不激活旧序列研究或继承其权重。两臂共同资格要求六控制及两新量可知且至少一个基础8字段已知；旧分钟正常缺失允许显式训练内中位数编码，全部训练缺失列停用。推理未知固定原槽，不删原Top50或用下一名补位。

## Feasibility / 事前无标签检查

证据根`X:/AIstock_temp/ADVISORY/p9-intraday-peer-synchrony-20261023/`。
spec SHA `07a06219e6011446a602b7571c1cc70f8f0ee9d70c93d8faa16738538ae0f046`；
screen receipt SHA `6b6e248df4c82db7b5b01b9da9f69eb7d55ac338632d93271f383625bac832f3`；
screen_rows SHA `4e11500f9d4a6522aaee2698f5ca73da150d4ef634b01fd1bbfd7aecf872c096`。

固定首个训练窗179D/17,900行、16,280唯一stock-D。17,854可用；41零方差、5共同边不足均UNKNOWN。两包179/179D均达到事前“至少80%行可算、至少80%包D合格”，179D均有横截面变化（事前最低20D、std>=1e-6）。78,502,080源bytes，95.757秒，峰值391,122,944bytes，0标签/0fit/0SQL。预算与支持规则冻结在spec后才读取；输入支持不代表成熟监督或经济支持。

绑定screen/spec/code/rows/factor/synthetic的SHA，冻结小元数据全hash、文件size/mtime租约和实际消费切片hash；不把stat冒称大bin全hash。完整prepare须与首窗17,900行身份、43旧分钟/6控制/2新量/UNKNOWN原因逐值对齐后才fit；身份、原因、计数精确一致，跨Windows/WSL浮点测量使用事前rtol=1e-12/atol=1e-10并另报是否逐bit相等，不容许经济差异。源数据CURRENT_DATABASE_NON_VINTAGE，D时钟约束不创造captured-at版本证明。

## Model / 唯一有限实验

三折固定train_end=2025-03-31/05-30/07-31，验证2025-04-01..05-30、06-01..07-31、08-01..09-30。先投影D及H（label_information_end），只训练D<=cutoff且H<=cutoff的成熟AVAILABLE/合法CASH_ENTRY_NOT_EXECUTABLE，现金目标真实0。两臂共同训练KEY/权重/编码。

Ridge(alpha100,lsqr,tol1e-6,max_iter2000,fit_intercept=False)，不调参/模型/seed/窗口/变换/符号。训练内逐列median与IQR；IQR=0且非恒定用population std，否则scale1。各列missing bit，编码后包/D中心化X/Y；D等权、D内包等权、包内监督行等权。逐折原子冻结两臂预测后才读该折成熟评价金融值。平手沿原rank；未知留原槽；保持十槽及原身份/分数/费用。

最多3×2=6真实监督fit；前219、完成后225fit＋1旧index。独占STARTED claim后才fit；未发布的中断fit不自动重拟合。注册源码commit/实现闭包/环境/父stage/绑定/源pins及screen。只读父prepared/train_finance且先H投影；拒绝outer及任何end/cutoff>2025-09-30；无final或sealed路径。无论结果均终止本候选，不自动第七fit或备用方向。

## Evaluation / 终点及停止规则

主终点candidate−原生Top10：原T开盘至T+4收盘固定十槽净cohort，买0.95/sell5.95bps各一次。合法不可入场现金0；真缺价/未成熟保留UNKNOWN。matched与系数仅解释，不追加门槛、回选matched或择包。固定126原评价D，三臂两包共同完整日配对，先包等权再D等权，不能把个股/跨包同股当独立样本。

沿原native分类：均值>0、至少2/3折正且各折可评价、共同成熟D覆盖>=80%、原生干预>=20D且>=20%成熟D并跨两折、尾部恶化<=100bps、额外UNKNOWN<=2pp才INNER_SUPPORTED；均值<=0或风险超限为INNER_NOT_SUPPORTED，其余INNER_INCONCLUSIVE。描述性95%区间跨零不追加否决。同步10D circular block/10000次/bootstrap seed20261021，报告区间/MDE、三折/两包、UNKNOWN/覆盖/干预、matched及系数。

即使SUPPORTED也只支持讨论新确认设计；当前305D多次消费，选择偏差、重叠5TD及NON_VINTAGE限制保留，0独立OOS/盈利确认。P4历史INNER_SUPPORTED、P5不推进确认、P6阴性、P8无fit停止均不改判。

## Scope / Implementation Plan

本文设计从main f30398f7b96be0f122f28bbf2ca0925935c20fbb独立PR交付，合入后从最新main另建源码树。精确源码六文件：

- backend/services/advisory_model_first/intraday_peer_synchrony_top10_5td_inputs_v1.py
- backend/services/advisory_model_first/intraday_peer_synchrony_top10_5td_v1.py
- backend/services/advisory_model_first/intraday_peer_synchrony_top10_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_intraday_peer_synchrony_top10_5td_v1.py
- 本文
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

复用真实接口：FrozenDataset/stamp、原minute_features/OLD_MINUTE、multiperiod输入身份、peer_residual的population/group_center/panel、peer_lead_lag的native summarize、economic_entry_pipeline原子stage、projected_finance和research_control registry/lock。从已验证float_share三叶模式复制为本次本地合同；不改共享源码或运行旧研究。reader在新输入叶内，不导入scratch screen。prepare恢复重新核消费切片hash与不可变准备stage；不重fit。

## Operations / Test Plan / Verification Plan

2CPU线程、无GPU、RSS<=2GiB、阶段<=1800秒、输出<=200MiB；输入<=305D/30,500行/300槽/300MB raw；screen另限179D/150MB。输出全上述X根，旧产物不可变。不查SQL、不复制大文件。新fit前只读公开QE running和WSL资源；UNKNOWN如实记，资源足够可并行，不控制其它任务。

直接合同覆盖留一减自身/40同群/共同覆盖、缺边与午休、零方差UNKNOWN、非法时钟/价格、股票置换/同比缩放、构造不等价、原人口与screen对齐、未来D/H毒化、共同监督/训练编码、固定未知槽、六claims/恢复/无outer、主终点不依赖matched或系数。行为稳定后一次直接小矩阵，Ruff/changed L0/ownership/diff/F1和蓝图F2，宽模块回归交必需CI。两轮同窗口自审，非独立外审；按既有授权提交/合入/自身官方清理，不控制后端。

## Design Acceptance Index

| ID | 验收目标 |
|---|---|
| F-491 | 查重、冻结无标签screen及证据边界 |
| F-492 | 同D留一测量、租约/切片身份与UNKNOWN完整人口 |
| F-493 | 共同成熟训练、57/59字段/固定三折六fit |
| F-494 | 原生净终点、预测屏障与无outer停止 |
| F-495 | 原子stage/claim/恢复与资源预算 |
| F-496 | 真实报告、精确范围及生产分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-491 | 本文Background/Feasibility | artifact: X:/AIstock_temp/ADVISORY/p9-intraday-peer-synchrony-20261023/screen_receipt.json；synthetic_receipt.json | DESIGN_AND_INPUT_PASS | none |
| F-492 | 计划inputs.synchrony/read_panel/verify_screen_alignment | backend/tests/advisory_model_first/test_intraday_peer_synchrony_top10_5td_v1.py（计划）；screen_rows.parquet | DESIGN_READY | none |
| F-493 | 计划core.training/fit/predict | backend/tests/advisory_model_first/test_intraday_peer_synchrony_top10_5td_v1.py（计划） | DESIGN_READY | none |
| F-494 | 计划pipeline.finance/fit_pair及core.summarize | backend/tests/advisory_model_first/test_intraday_peer_synchrony_top10_5td_v1.py（计划）；本文Evaluation | DESIGN_READY | none |
| F-495 | 计划pipeline.Study/preregister/run | backend/tests/advisory_model_first/test_intraday_peer_synchrony_top10_5td_v1.py（计划）；本文Operations | DESIGN_READY | none |
| F-496 | 本文Scope/Production Gates | backend/tests/advisory_model_first/test_intraday_peer_synchrony_top10_5td_v1.py（计划）；本文Non-goals | DESIGN_SCOPE_READY | none |

设计自审一核对自包含留一、共同边覆盖、共同控制、旧单股与日频查重边界及非vintage；第二轮核对人口/成本、共同支持、先预测后评价、6fit与全部终态停止。源码两轮同窗口自审修复恢复时重复特征拼接，补齐分钟测量依赖合同hash，并在读完整输入/标签前明确跨节点浮点容差。15项直接测试分别14项基础矩阵、1项新增实际文件读取；容差改动后仅重跑直接对齐项。L0三个P2复杂度提醒已审：唯一stock-D×5字段切片<=300MB、包D×50共同计算<=305D、监督merge one_to_one<=30,500行；0blocking，不修改公共baseline。真实结果及源码必需CI另报。

## Non-goals / Risks / Production Gates / DESIGN-COMPLIANCE-001

原Top50成员关系可能带选择偏差，同群不是市场，相关不是因果；分钟bar缺失/精度及当前历史修订可能影响代理。低预测能力/无增量均为允许终态；不按结果改定义、窗口或包权重。

四项逐一检查：设计完整但源码/研究待实现明确分态；错误与UNKNOWN不伪成功；固定业务/费用/人口/验收不改；无设计外审批或门禁。只Advisory离线，不改QE/HMM/LocalSIM/Selection/StrategyPackage/数据发布/公共workflow。production_ddl_gate=noop、dependency_gate=noop、activation=noop、backend_restart_required=false；无DB/DDL/DML、修数/采集、生产回放或服务控制。
