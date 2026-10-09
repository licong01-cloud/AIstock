# QE 共享因子指标引擎可选多期限研究评价设计

版本：v1.0；日期：2026-10-10；分类：F1 单模块扩展；状态：实现及本地兼容验证通过，PR/CI 与运行态分别验收。

## 背景

因子研究需要可选 40/60/120/240 个交易日评价，区分短期反转与中长期延续。用户授权 QE 交付共享引擎及兼容测试，因子研究窗口待接口交付后再接入。不改变既有四周期、官方指标或 QE 实验，不建设另一套回测引擎。

## 范围与 ownership

生产文件仅：backend/services/quantevolver/qe_eval_v2_metric_engine.py、backend/services/quantevolver/qe_eval_v2_qlib_reader.py（提供现有冻结日历读取接口）。

改动测试仅：backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py、backend/tests/quantevolver/test_official_evaluation_cache_source.py。原 test_factor_metrics_h20_contract.py、test_factor_st_pit_metrics_cache.py、test_official_factor_batch_compute.py 仅作不改动的回归目标。上述生产/测试归 qe.core；本设计文档为本 Feature 的交付合同。新增合同测试放在现有 QE PR 计划实际收录的 quantevolver 目录，不修改 CI 配置或降低覆盖要求。

## 非目标

不修改研究 runner/comparison、官方 writer、评级/UI、生产因子状态、训练标签、模型/因子公式、调仓/执行策略、数据集、工作流或开发规范。不计算长周期真实组合收益/夏普，不重算全库或启动 QE 实验，不新增人工门禁或审批。不清理原任务工作树或资产。

## Design Acceptance Index

- F-001：默认四周期及官方字段/数值不变，显式四周期与原调用业务输出等价。
- F-002：请求级 holding_periods 支持 1/5/10/20/40/60/120/240 日，不修改全局；基础四周期保留 1d/20d 内部依赖。
- F-003：研究长周期沿用 T+1 到 T+h+1，按冻结完整交易日历移位，不填补缺价、不跨股票压缩。
- F-004：各期限独立有效样本；未成熟、缺价、因子不可用、截面不足分开，共同成熟支持信息不替代独立指标。
- F-005：新请求后默认调用及并发不同请求不污染；价格一次准备复用，默认不承担长周期计算成本。
- F-006：官方写入和评级合同不变；研究期限指标不写入官方表，不伪造长周期收益/夏普。
- F-007：直接接口回归、默认基线比较、多轮审核及持久源码交付；合入和运行态明确分离。

## 实施方案与接口合同

### 请求

prepare_shared_context 在既有参数后增加 keyword-only holding_periods，接受非空整数期限序列。None 保持原四周期；显式四周期保持同样数值与结果字段。允许期限仅为 1、5、10、20、40、60、120、240，去重后按日数排序。非法类型/值是输入错误，不增加研究准入或人工配置要求。

请求元数据仅放入显式配置上下文：holding_periods（请求名称→shift）、computed_holding_periods（基础四周期与请求并集）、label_calendar（读价日历）。例如只请求 60 日，holding_periods 为 60d→61，但计算基础矩阵仍包含四周期。调用方不得从名称自行猜测 shift。

compute_single_factor_metrics(..., include_horizon_metrics=True) 的研究期限视图只返回请求期限；原有扁平 rank_ic_1d/5d/10d/20d、1d 分组指标及 H20 伴随合同保留。新增期限不扩入扁平官方字段；不传研究配置时原结果字段不增减。

### 因子研究窗口交接

请求使用 prepare_shared_context(..., holding_periods=[40, 60, 120, 240])，然后 compute_single_factor_metrics(..., evaluation_windows=..., include_horizon_metrics=True)。None 与不传参数均为原四周期；仅传 [60] 时嵌套期限指标只有 60d，基础四周期仍在上下文中用于兼容诊断。include_horizon_metrics=False 不输出新研究字段。

| 期限名称 | 持有交易日 | shift / T+出场偏移 |
|---|---:|---:|
| 1d | 1 | 2 |
| 5d | 5 | 6 |
| 10d | 10 | 11 |
| 20d | 20 | 21 |
| 40d | 40 | 41 |
| 60d | 60 | 61 |
| 120d | 120 | 121 |
| 240d | 240 | 241 |

每个 evaluation window 的 horizon_metrics[期限] 固定提供 ic_mean、ic_std、icir、rank_ic_mean、rank_ic_std、rank_icir、n_effective_days。新增长周期请求同时提供 horizon_support[期限]：status、holding_days、entry_offset、exit_offset、n_requested_days、n_mature_days、n_immature_days、n_missing_price_pairs、n_factor_missing_pairs、n_valid_pairs、n_insufficient_cross_section_days、mature_start/end、effective_start/end。status 为 ok、unmatured、no_eligible_samples、price_missing、factor_unavailable、insufficient_cross_section（无日期的窗口沿用原 skipped report）。没有有效值的指标为 None，不填 0。

common_horizon_support 仅报告所请求期限的共同成熟/有效样本和日期，不重新计算共同样本 IC、不替代各期限。研究窗口后续修改 runner/comparison/report 时应从 ctx 的 holding_periods 和 label_calendar 获取期限与成熟规则；切信号窗口前生成标签，切后保留原 label_calendar，不能用切后长度判断成熟。研究端接入由因子窗口另行实施，本轮不改其源码。

### 时间、缺价及样本

长周期 opt-in 上下文只读已有 day.txt，把读取价格重索引到声明区间完整日历；缺行保持 NaN，不复制价格。默认和显式四周期走旧兼容分支；完整日历研究分支只影响新请求，不顺手修订旧官方计算。因子窗口先生成标签再切信号区间，保留 label_calendar 及期限映射以判断成熟。

PIT 与停牌掩码沿用既有信号日样本语义；长周期标签仅是两个端点 close 的预测评价，不模拟持有期间成交、估值或路径风险。无价格不能压缩交易日，不能把累计标签解释为可成交组合收益。

研究结果单独提供各期限 horizon_support 与 common_horizon_support：报告成熟及有效起止日、请求/成熟/未成熟日数、有效价格对/缺价对、因子不可用及截面不足。共同视图仅描述请求期限共同成熟和有效样本，不能用交集覆盖原期限结果。无数据返回 None 及原因，禁止补 0。

### 统计、性能与保存

复用现有稳健 Pearson IC、RankIC、ICIR/RankICIR 定义和有效日期数。长周期 ICIR 是描述统计，不照搬 h20 的 HAC lag=19，不生成长周期策略夏普或年化收益。现有 top_* 是 1d 分组统计，不能按研究请求重新命名。

价格/PIT 准备一次复用；新增矩阵只属于显式请求，逐期限计算且不新增线程池；共同支持逐期限切片，禁止一次性复制全部期限矩阵。理论上每新增一期的收益矩阵需 日期数×股票数×元素字节数（float32 为 4，float64 为 8），排名与掩码另需临时内存；本轮未运行全历史性能基准，不把理论估算当作实测。无期限指标缓存，不把期限加入原始价格缓存键。研究上下文和嵌套输出不传官方 writer；官方入口继续默认调用，既有数据表及评级不变。

## 验证方案

在 tmp_path 合成 Qlib close Bin/day.txt 与 PIT/停牌假输入，不读取真实数据或数据库。公共 prepare→compute 测试覆盖四个新增期限、60 日单请求、默认/显式四周期、上下文复用与并发、刚好成熟/差一日、节假日、空日/单股缺价/停牌、研究窗口裁切、无有效截面与共同支持。基线比较使用修改前提交源码及同样输入，只剔除 UUID/时间/耗时等非确定字段。

官方单/批量调用及 writer 消费用已有隔离合同测试；广 QE 回归按 changed files→ownership→qe_read_backend 交 CI/验证中心，不启动完整 QE/qrun。所有测试输出在临时目录，禁止向 source worktree 写运行日志、checkpoint 或正式资产。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | qe_eval_v2_metric_engine.py 默认分支 | backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_optional_horizons_preserve_default_outputs_and_concurrent_requests；修改前基线 17 组相等（下节） | 通过 | 无 |
| F-002 | prepare_shared_context 请求规范化 | backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_optional_horizon_mapping_is_detached_sorted_and_keeps_internal_dependencies | 通过 | 无 |
| F-003 | qe_eval_v2_qlib_reader.py 日历读取；标签矩阵 | backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_optional_long_horizons_use_public_api_and_exact_exit_date；backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_research_calendar_preserves_missing_price_dates_and_explicit_bounds | 通过 | 无 |
| F-004 | horizon_support/common_horizon_support | backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_long_horizon_maturity_does_not_crop_short_horizons_or_sliced_context；backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_long_horizon_preserves_pit_and_suspend_masks_without_compressing_dates | 通过 | 无 |
| F-005 | 请求局部上下文；默认矩阵数量 | backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_optional_horizons_preserve_default_outputs_and_concurrent_requests；backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py::test_long_horizon_calendar_gaps_and_per_stock_missing_prices_remain_nan | 通过 | 无 |
| F-006 | 保持默认官方保存链路 | backend/tests/quantevolver/test_official_evaluation_cache_source.py::test_compute_local_reads_backtest_cache_without_snapshot_or_pipeline；backend/tests/quantevolver/test_official_factor_batch_compute.py::test_compute_aborts_remaining_batches_after_resource_gate_failure；backend/tests/test_factor_metrics_h20_contract.py::test_official_metric_upsert_accepts_new_and_legacy_payloads | 通过 | 无 |
| F-007 | 完整期限公共入口及反污染回归 | pytest backend/tests/quantevolver/test_qe_eval_v2_metric_engine.py -q；pytest backend/tests/test_factor_metrics_h20_contract.py backend/tests/test_factor_st_pit_metrics_cache.py backend/tests/quantevolver/test_official_evaluation_cache_source.py backend/tests/quantevolver/test_official_factor_batch_compute.py -q | 通过 | 无 |

## 已执行验证及审核

2026-10-10 本地定向矩阵：95 passed、1 skipped。跳过的是原有 Windows 不支持的 fork timeout 测试，长周期及默认兼容测试无跳过。用修改前基线 3bda2b7760ad8893af4dcdadea96790628fca78d 的引擎源码与当前实现，在同一 580 日期×12 股票的合成价格、因子、PIT 及停牌输入上比较，17 组完全相等：两因子×默认/显式四周期×原窗口/显式窗口×include_horizon_metrics 两态，以及 compute_all_factors_metrics 默认批量入口。仅剔除 UUID、时间、耗时；比较所有业务字段、数值与缺失值语义。原/新源码同有预存的全 NaN 统计 RuntimeWarning，没有吞掉测试失败或修改生产告警行为。

第一轮自审已修复多矩阵同时复制；第二轮核对默认/官方保存、时间成熟、PIT/停牌、并发隔离及不伪造长周期组合收益；第三轮核对实际 changed files、F1、静态检查及最终源码。广 QE 间接调用链回归交既有 CI/Nightly，不冒充完整 QE 实验验收。本节是执行结果，不新增任何业务准入要求。

测试落位调整后新增 QE 合同模块 24 passed，原两份顶层旧测试没有源码变更。F1：7/7 通过；Ruff、py_compile、diff-check 通过；nox l0 与 guardrail_changed_files 通过，实际五文件 ownership mapped=5、unmapped=0、ambiguous=0。先前 F1 指出旧测试文件未被 PR 精选计划收录，已通过把本功能新增测试放入同一 QE owner 下实际执行的测试目录解除；未修改或放宽流水线。

## 风险与失败模式

长周期标签尾部无法成熟是日期限制，不是缺失数据补齐任务；缺价和成熟分开报告。长周期使矩阵/排名计算增加，显式研究任务承担，普通 QE 和官方默认不增加该计算。研究成员必须保留共享上下文的日历与期限映射，不能重新生成标签或调用官方 writer。完整日历研究分支与旧兼容分支的区别必须明确，不能宣称改写了旧历史结果。

## 生产门禁与发布

DDL/DML=noop；依赖安装=noop；数据集写入/激活=noop；实验启动=不执行；进程控制=false。按当前 runtime catalog，修改 backend 共享源码属于 backend-main。源码、CI、合入、节点研究代码可用和后端运行态分别报告；backend 重启由用户执行。不为研究新增手工字段、环境锁或审批门禁。

## DESIGN-COMPLIANCE-001

逐项审核：① 完整实现全部四个已批准新期限和公共入口，真实 tmp_path Bin 经 prepare→compute 验证，禁止 mock-only 交付；② 未成熟、缺价、因子缺失、截面不足显式且不补零，不回退旧期限；③ 17 组修改前基线及官方合同证明默认语义不变，模型/策略/数据集不修改；④ 不新增人工配置、环境锁或审批，期限合法性与输入日历校验是算法输入校验，不是额外准入门禁。源码合入不冒充后端运行态完成。
