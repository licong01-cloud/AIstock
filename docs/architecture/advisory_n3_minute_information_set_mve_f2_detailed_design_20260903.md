# Advisory N3 分钟信息集 Learnability MVE F2 详细设计

> 版本：v1.2
> 日期：2026-09-03  
> Feature tier：F2  
> 状态：IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO
> objective contract：`ALPHA_RANKING`  
> study type：`LEARNABILITY_AUDIT`  
> decision use：`NAVIGATION_ONLY`

## 1. Background / 背景与事实入口

1. N3 上游 QE Alpha MVE、父包增量 overlay 和腿间共识/分歧 MVE 均已完成且 `selected=0`。最近一次正式腿间 bundle 为 `42ac23b6...`，完成 `2/2/2/0`，380 个干预日说明失败不是覆盖或恒等输出造成，而是新增交互没有经济增量。
2. 腿间 expanded 相对 parent 的 RankIC delta 与 Top5 成本后 lift 分别为 `-0.003564` 和 `-151.22 bps`；expanded 与 parent 的日均 score Spearman 仍为 `0.98209`。继续更换同一日线信息集的 feature 组合、loss 或模型族不构成新假设。
3. 正式 route 已唯一指向 `N3_MINUTE_INFORMATION_SET_MVE`。本任务只引入 T 日收盘时已可见的盘中路径信息，不并行启动 Entry、Exit、N2-B 包或另一个模型族。
4. target-free/PIT spike 已确认活跃源为 WSL `/home/lc999/data/qlib_minute_bin`，Qlib `0.9.6.99`，snapshot id 为 `qlib_minute_authoritative_full_candidate_20240102_20260630`。日历覆盖 `2024-01-02 09:30:00..2026-06-30 15:00:00`，完整包围 N2-A 的 386 个决策日。
5. N2-A key-only 读取确认 1,710,301 行、4,503 只股票，时间为 `2024-07-04..2026-02-02`；spike 没有读取 outcome/target 列。全 386 日 target-free 扫描原始结果为 1,695,153 complete、13,473 partial、1,675 whole-day missing。
6. 13,473 个 raw partial 中，13,461 个来自 `2025-11-27/2025-12-08/2025-12-12` 三个 241-slot session：每个候选都只有 240 个合法 OHLC bar。source spike 以 bar-count 归一化后得到 1,708,614 complete、12 partial、1,675 whole-day missing，complete fraction `0.999013624`、any-bar availability `0.999020640`。实现期真实 Qlib 烟测进一步确认，这不是严格意义上“所有股票同一个 slot 都为空”：以 `2025-12-08` 为例，4,474 只股票缺 `13:00`，另 13 只缺 `11:30` 且在 `13:00` 有值。因此正式实现必须同时保留 raw `240/241` coverage 与独立 `SESSION_WIDE_SINGLE_BAR_DEFICIT` 归一化分类，不能伪造一个全市场空 slot，也不能移动、补零或删除 bar。扫描耗时 1,282.87 秒，峰值 RSS 896,438,272 bytes，source-ready receipt 为 `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_source_spike_v1_20260903/source_spike_receipt.json`，SHA256 `20b2f7639ffa6467f056e47b4decd77f0ab39650ae1d8d6d04226a9e20aebbc2`。
7. 本 MVE 的分钟聚合公式不读取 LSTM/FUND 腿，工程接口可被其它包复用；但本轮证据仍严格绑定当前 N2-A 父包候选、parent score 和 policy。它不把单包结果冒充跨包通用模型，跨包共享仍须至少两个兼容包的独立 exact bundle 证据。
8. 正式 request `advn3minreq_333ff0cf8e102efef88961c3` 已在 clean `main@a82a58ce...` 完成，bundle `0076a3a6...` 为 `2/2/2/0`。384 个可评价日全部发生真实干预，但 candidate RankIC/Top5 净超额分别为 `0.09020/128.31 bps`，低于 parent/comparator 的 `0.12284/443.65 bps`；四项 family-wise 门槛全部失败，故 `selected=0`，唯一 next task 为 `N3_QE_ALPHA_GENERATOR_MVE_DESIGN`。

## 2. Scope / 目标与成功边界

交付一个固定信息集、固定简单模型族和严格 cross-fit 的分钟 learnability MVE，只回答：

> 在当前父包分数之外，T 日盘中路径、波动、成交额时段分布和涨跌停停留信息，是否能在同一 PIT/H20/成本政策下产生可学习的增量 Top5 净超额？

交付范围：

1. 冻结腿间正式结果、N2-A、N1 CPCV 和分钟 snapshot 四组来源身份。
2. 按决策日流式读取分钟 Bin，为全部 1,710,301 个 N2-A 键生成固定分钟聚合和 typed coverage；不得由分钟源重建股票池。
3. 比较一个 parent-only Ridge comparator 与一个 parent+minute Ridge candidate；模型、超参、fold、标签和预处理均冻结。
4. 为全部 source row 生成恰好 7 个 OOF prediction 的均值；只有 1,705,332 个 finite-evaluable row 可进入训练。
5. 报告相对 current parent 和 parent-only comparator 的配对 RankIC、Top5 成本后 lift、干预支持、换手、MDE 和 family-wise 区间。
6. 发布 immutable bundle、append-only registry、单页 route、inspect 和 exact retry no-op。

成功最多产生一个 `NAVIGATION_ONLY` candidate 和独立 confirmation 设计入口；不生成 final estimator、可部署模型、StrategyPackage、运行时权重或交易输入。

## 3. Non-goals / 非目标与禁止项

- 不搜索分钟窗口、特征子集、Ridge alpha、solver、seed、fold、标签、方向、阈值或缺失处理。
- 不加入 T+1 open/current 或任何 target 日分钟数据；这些信息只属于 Entry Guard。
- 不读取 sealed holdout，不把开发窗口称为 future OOS、confirmation 或 activation evidence。
- 不访问数据库、网络、Tushare、Qlib daily、实时行情或执行结果；只读 request 绑定的本地 minute snapshot。
- 不从分钟 `instruments/all.txt` 重建 universe，不以停牌、整日无行情、部分分钟缺失删除股票或日期。
- 不写因子库、StrategyPackage、Selection、Advisory runtime、descriptor、仓位、Paper/QMT 或订单。
- 不启动、停止或重启后端/Worker；不执行 DDL/DML；不引入通用分钟特征平台、缓存平台或调度器。
- 不保存 final-refit estimator/joblib，不接 API/UI，不把两个模型的 fold/path 误计为独立发现。

## 4. Architecture / 架构与数据流

```text
N3 leg formal selected=0 receipt + route
                   |
N2-A full_universe_signal_outcomes.parquet
  keys + parent score + frozen H20 outcome
                   |
N1 n1_label_interval_cpcv.json
  8 blocks / 28 READY paths / 20d embargo
                   |
immutable minute snapshot fingerprint
  T-day only, stream one decision date at a time
                   |
fixed raw aggregates -> same-date ranks -> typed missing flags
                   |
parent-only comparator + parent/minute candidate
  2 frozen Ridge trials / train-fold impute+scale
                   |
7-path mean OOF per source row
                   |
paired daily economics / support / MDE / family-wise intervals
                   |
immutable bundle -> registry -> route
```

`prepare` 与 `run` 均只能在 WSL `rdagent-gpu` 执行。正式 request 必须来自 clean `HEAD == origin/main`。运行时只消费 request 绑定的本地文件和 snapshot，不扫描 latest、不查询外部源。target-free spike 仅证明 source readiness，不替代正式 request 的 content fingerprint 或正式 MVE。

## 5. Contracts / 输入与身份合同

### 5.1 必需 bundle

1. N3 腿间 bundle `42ac23b6...`：必须 inspect valid、`2/2/2/0`、`selected=0`、`next_task=N3_MINUTE_INFORMATION_SET_MVE`、`NAVIGATION_ONLY`，sealed/deployable/runtime/business-write 全 false；只消费 `request.json`、`learnability_receipt.json`、`registry_record.json` 和 `manifest.json`。
2. N2-A bundle `6784df1a...`：必须通过既有 readback；只消费 `full_universe_signal_outcomes.parquet`、`request.json`、`audit_receipt.json`、`registry_record.json` 和 `manifest.json`。
3. N1 bundle `74827d03...`：必须 inspect valid；只消费 `n1_label_interval_cpcv.json`、`learnability_daily.parquet`、`request.json`、`manifest.json` 和 split identity。`learnability_daily` 只提供冻结 regime 映射。

### 5.2 分钟 source identity

request 必须绑定：

- provider URI：`/home/lc999/data/qlib_minute_bin`；
- `meta_export.json` 的 SHA256 与 snapshot id；
- `calendars/1min.txt` 的 SHA256、144,688 行及起止 timestamp；
- `instruments/all.txt` 的 SHA256、PIT span 语义及行数；
- N2-A 4,503 instrument 对应的八个必需字段文件：`open/high/low/close/volume/amount/limit_up/limit_down.1min.bin`；
- 每个必需 Bin 的 relative path、size 与 content SHA256，并对排序后的 inventory 形成 `minute_source_content_sha256`。

`prepare` 只可读取 N2-A 的 key 两列确定 exact instrument set，不可读取标签统计。`run` 在聚合前重新计算相同 inventory digest；任一文件缺失、内容 hash、calendar、manifest、snapshot 或字段 roster 漂移均 fail closed。mtime 只作 telemetry，不构成内容身份。

### 5.3 主键、窗口、标签与候选集合

- 唯一键：`decision_as_of_trade_date + instrument`。
- 开发窗口：`2024-07-04..2026-02-02`，386 日。
- 候选集合严格来自 N2-A 1,710,301 个键；分钟 manifest 的 PIT span 只作 source coverage 说明，不增删候选。
- parent score：`score__IC_WEIGHTED_PARENT`，必须全 finite，排序语义继承 N2-A。
- 标签：N2-A 的 `economic_net_excess_bps`。1,709,387 个 known 中 1,705,332 个 finite 可训练；4,055 个 known-nonfinite 和 914 个 unknown 继续保留在候选、OOF 输出及逐日排序中。
- H20、T+1 open 入场、成本、capacity haircut、benchmark、PIT universe、baseline/shadow policy identity 全部继承 N2-A/N1，不重新定义。
- 任一 Top5 成员标签不可评价时，该模型当日 Top5 指标 typed unavailable；不得用少于 5 只股票的均值替代。

### 5.4 组合 dataset identity

`dataset_identity` 是腿间 route identity、N2-A dataset identity、N1 split identity、minute snapshot/content identity 和全部 evidence ref 的 canonical hash。任一 bundle、文件、row count、schema、policy 或关系漂移均拒绝运行。

## 6. 决策时钟、分钟聚合与缺失合同

### 6.1 时钟

Advisory ranking 在 T 日收盘后产生，目标入场日为下一交易日。分钟特征只允许读取 `decision_as_of_trade_date=T` 的本地 calendar slot，最大 timestamp 为 `T 15:00:00`；任何 `timestamp.date > T`、T+1 open/current、高低收或执行结果直接触发 PIT failure。

### 6.2 原始字段与 bar 合法性

只读取八个字段：`$open/$high/$low/$close/$volume/$amount/$limit_up/$limit_down`。价格必须 finite 且大于 0；volume/amount 必须 finite 且不小于 0；非空 limit flag 必须严格为 `0/1`。calendar 决定当日 raw expected slots，不能写死 240，因为 source 中历史日存在 241-slot 语义。若某一 slot 在该日全部 N2-A 股票中都不存在合法 OHLC bar，则记录真实 `MARKET_WIDE_EMPTY_CALENDAR_SLOT` 并从股票级 effective expected slots 分母中剥离；volume/amount/limit 占位值不能把无 OHLC 的 slot 冒充为合法 bar。另对三个已冻结的 241-slot 日期，只有在当日每个 N2-A 候选都恰好拥有 240 个合法 OHLC bar 时才记录 `SESSION_WIDE_SINGLE_BAR_DEFICIT`：raw coverage 仍为 `240/241`，同时在 coverage receipt 中报告归一化 session 分类；不得声称具体 slot 全市场为空，不得移动、填充或删除任一股票 bar。若整日 effective slots 为 0，全部股票仍按 whole-day missing 保留。

- `minute_valid_bar`：OHLC 全 finite 且大于 0。
- `minute_available`：当日至少一个 valid bar 时为 1，否则为 0。
- `minute_coverage_fraction`：valid bar 数 / 当日 effective expected slots。
- 整日无行情：八个经济聚合保持 NaN，availability=0、coverage=0，候选仍保留。
- 部分缺失：保留真实 coverage；只在 coverage `>=0.80` 且公式所需窗口有数据时计算经济聚合，否则经济聚合为 NaN。不得缩短成另一个窗口或填零冒充完整路径。

### 6.3 固定八项经济聚合

以下公式全部只使用 T 日 valid/finite 值，收益和波动统一乘 10,000 转为 bps：

1. `opening_30m_return_bps`：`[09:30,10:00]` 内最后 finite close / 第一 finite open - 1。
2. `closing_30m_return_bps`：`[14:30,15:00]` 内最后 finite close / 第一 finite open - 1。
3. `realized_volatility_bps`：相邻 calendar slot 的 finite close log return 平方和开根；跨缺口 pair 不计算。
4. `directional_efficiency`：全日最后/第一 finite close 的 log return，除以相邻 slot log return 绝对值之和；分母为 0 且全日路径 flat 时固定为 0。
5. `close_to_vwap_bps`：最后 finite close / `sum(amount)/sum(volume)` - 1；总 volume 或 VWAP 非正时为 NaN。
6. `opening_30m_amount_share`：`[09:30,10:00]` amount / 全日 amount；全日 amount 非正时为 NaN。
7. `closing_30m_amount_share`：`[14:30,15:00]` amount / 全日 amount；全日 amount 非正时为 NaN。
8. `limit_pressure`：可用 slot 的 `mean(limit_up)-mean(limit_down)`。

窗口、边界、顺序和公式进入 schema hash，不存在可选特征。future/label poison 测试必须证明修改 outcome、exit、T+1 及以后字段不改变任何分钟聚合或 schema hash。

### 6.4 同日 rank 与最终模型特征

八项经济聚合分别在当日 finite 成员中执行 `rank(method="average", pct=True, ascending=True)`，缺失仍为 NaN，不跨日归一化。

`MINUTE_PARENT_COMPARATOR_V1` 固定 1 项：

1. `parent_rank_pct`

`MINUTE_INFORMATION_EXPANDED_V1` 固定 11 项：

1. `parent_rank_pct`
2. `minute_available`
3. `minute_coverage_fraction`
4. `opening_30m_return_rank_pct`
5. `closing_30m_return_rank_pct`
6. `realized_volatility_rank_pct`
7. `directional_efficiency_rank_pct`
8. `close_to_vwap_rank_pct`
9. `opening_30m_amount_share_rank_pct`
10. `closing_30m_amount_share_rank_pct`
11. `limit_pressure_rank_pct`

经济 NaN 只允许由 train-fold median imputer 处理，并由 availability/coverage 显式保留缺失语义；不得全局拟合、目标条件填补或以 0 填充经济特征。任一训练 fold 某经济列全缺失时 fail closed，不静默删列。

## 7. 固定模型与 cross-fitting

### 7.1 两个诚实计数的 trial

| trial | 作用 | 特征 | 可选择 |
|---|---|---|---|
| `N3_MINUTE_PARENT_RIDGE_COMPARATOR_V1` | 隔离“仅重新拟合 parent”带来的变化 | §6.4 comparator | 否 |
| `N3_MINUTE_INFORMATION_EXPANDED_V1` | 检验分钟信息的增量 learnability | §6.4 expanded | 是，最多一次 |

`planned/generated/evaluated=2/2/2`。两者和全部 path 统一使用：

- `sklearn.impute.SimpleImputer(strategy="median")`，仅 fit train；
- `StandardScaler`，仅 fit train；
- `sklearn.linear_model.Ridge(alpha=100.0, solver="lsqr", fit_intercept=True)`；
- 原始 `economic_net_excess_bps` 标签，不按结果 winsor、重采样、调方向；
- bootstrap seed `20260903`；无 final refit。

### 7.2 CPCV

- 精确复用 N1 的 8 block、28 READY path、20 日 embargo。
- train/validation 日期不相交；imputer/scaler/Ridge 只 fit train finite-evaluable rows。
- 全部 1,710,301 个 source row 各作为 validation 恰好 7 次，按 row 累加 prediction sum/count；不 materialize 28 份完整副本。
- comparator/candidate 使用相同 row、fold、标签与 preprocessing class；任何 multiplicity、非有限 prediction、顺序或 key 漂移直接失败。

## 8. 评价、支持度与一次选择

### 8.1 每日配对指标

对 current parent、parent-only comparator 和 minute candidate 每日计算：

- Spearman RankIC；
- Top5 `economic_net_excess_bps` 均值与 `top5_evaluable`；
- Top5 instrument set、candidate 相对 parent/comparator replacement count；
- 相邻决策日 Top5 churn；
- raw 与 session-normalized complete/partial/whole-day-missing 数、coverage 分布、session-wide deficit 日期和各特征 finite fraction。

candidate 同时报告相对 parent 和 comparator 的 RankIC delta 与 Top5 lift。推断固定为 20 日 moving-block bootstrap、2,000 repetitions、seed `20260903`。四个主比较（2 指标×2 baseline）使用 Bonferroni `alpha=0.05/4`；model trial 数仍为 2，并报告 DSR 诊断但不以 DSR 替代经济门槛。

### 8.2 干预支持

candidate Top5 与对应 baseline Top5 不同才算 intervention。对 parent 和 comparator 分别要求：

- evaluable days `>=382`；
- intervention days `>=60`；
- intervention fraction `>=0.25`；
- N1 中每个实际出现 regime 的 intervention days `>=20`。

分钟 missing 不减少候选数、日期数或 OOF 数；coverage 只作为独立支持事实报告。稀疏干预的区间继续使用按日 block 方法，不把股票行当独立样本。

### 8.3 Candidate eligibility 与固定分流

candidate 只有同时满足以下条件才可一次选中：

1. source fingerprint、PIT、feature、CPCV、OOF、row coverage 和 parent parity 全通过；
2. 对 parent 和 comparator 的干预支持均满足 §8.2；
3. 相对 parent 的 family-wise RankIC delta 下界 `>0`；
4. 相对 parent 的 family-wise Top5 net lift 下界 `>5 bps`；
5. 相对 comparator 的 family-wise RankIC delta 下界 `>0`；
6. 相对 comparator 的 family-wise Top5 net lift 下界 `>0`；
7. 无非有限、退化、identity、PIT 或资源错误。

`selected=1` 的唯一 next task 为 `N3_MINUTE_INFORMATION_SET_CONFIRMATION_DESIGN`；`selected=0` 的唯一 next task 为 `N3_QE_ALPHA_GENERATOR_MVE_DESIGN`。同一 frontier 不得回选分钟窗口、特征、阈值或 alpha；这里的 generator MVE 是新因子生成假设，不是重跑已关闭的 24-proposal 归档信号 screen。

## 9. Artifact、registry 与 route

bundle 固定成员：

- `request.json`
- `minute_source_inventory.parquet`
- `source_identity_receipt.json`
- `feature_schema.json`
- `minute_coverage_daily.parquet`
- `minute_feature_panel.parquet`
- `oof_score_panel.parquet`
- `fold_diagnostics.parquet`
- `daily_metrics.parquet`
- `model_summary.json`
- `frontier_receipt.json`
- `resource_report.json`
- `learnability_receipt.json`
- `registry_record.json`
- `manifest.json`

manifest 绑定成员 SHA256、size 和 parquet row count。临时目录完成全量 readback 后原子发布；exact retry 必须复用同一 bundle，registry duplicate no-op、route exact no-op。

registry：

- experiment：`ADVISORY-N3-MINUTE-INFORMATION-LEARNABILITY-V1`
- stage：`N3_MINUTE_INFORMATION_SET_MVE`
- study：`LEARNABILITY_AUDIT`
- objective：`ALPHA_RANKING`
- decision use：`NAVIGATION_ONLY`
- unique variable：`FIXED_PARENT_ONLY_VS_FIXED_T_DAY_MINUTE_PATH_INFORMATION`
- trial count：`2/2/2/0|1`
- consumed window：`P0C_DEVELOPMENT_CONSUMED_20240704_20260202`

source spike 是 target-free readiness evidence，不计 model trial；正式 MVE 的两个模型如实进入 lineage 累计 trial 数。route 只记录研究导航，不构成激活或交易输入。

## 10. 资源、错误与安全边界

- concurrency=1；RSS 上限 8 GiB；临时输出上限 16 GiB；wall time 仅 telemetry，固定 `null`，不自动停止。
- 分钟数据严格按单日、所需股票和八列投影流式聚合；不得把 33GB raw minute 全量同时 materialize 到内存。
- source spike 的 386 日扫描耗时 21.38 分钟、峰值 RSS 896 MB；正式实现不得以性能为由缩小日期、股票或字段 roster，也无需为此建设缓存平台。
- 正常 whole-day/partial missing 形成 typed coverage；不得 broad exception 后继续成功。
- typed reason code：
  - `ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID`
  - `ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH`
  - `ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID`
  - `ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE`
  - `ADVISORY_N3_MINUTE_MVE_CPCV_INVALID`
  - `ADVISORY_N3_MINUTE_MVE_OOF_INVALID`
  - `ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED`
  - `ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID`
  - `ADVISORY_N3_MINUTE_MVE_RESOURCE_LIMIT_EXCEEDED`

## 11. Implementation plan / 实现文件与顺序

精确源码范围：

- `backend/services/advisory_model_first/minute_information_set_contracts.py`
- `backend/services/advisory_model_first/minute_information_set_pipeline.py`
- `scripts/advisory_minute_information_set_mve_run.py`
- `backend/tests/advisory_model_first/test_minute_information_set_contracts.py`
- `backend/tests/advisory_model_first/test_minute_information_set_pipeline.py`
- `backend/tests/advisory_model_first/test_minute_information_set_delivery.py`
- `scripts/ci_change_classifier.py`（仅新增 CLI direct mapping）
- `backend/tests/scripts/test_ci_change_classifier.py`（仅验证 direct mapping）
- 本设计和主蓝图

实现顺序：source fingerprint/streaming aggregate → contract/schema/poison → train-fold missing/CPCV OOF → paired evaluator → immutable delivery/CLI → 正式只读预检 → 重复审核。不得为复用而重构既有 N1/N2/N3 平台。

## 12. Verification plan / 验证计划

1. 合同：两个 trial、八字段、八聚合、11 项 expanded 特征、阈值、false gates 和 next-task 不可 override。
2. Source：leg/N2-A/N1 hash 与关系；minute meta/calendar/manifest/content inventory 漂移 fail closed；multi-span manifest any-match 正确。
3. PIT：只读 T 日至 15:00；T+1/future/label poison 不改变 feature；分钟 manifest 不增删 N2-A key。
4. Missing：whole-day 和 partial 均保留所有候选；经济值不填零；train-only median；全缺列 fail closed。
5. Formula：240/241 slot、窗口边界、adjacent-return、VWAP、amount share、limit flag 和 flat-path 语义逐值测试。
6. CPCV：28 READY、20 日 embargo、train/validation 隔离、每 row 恰好 7 OOF。
7. Evaluation：parent parity、paired RankIC/Top5、family-wise interval、双 baseline support/MDE/churn 和 0/1 分流。
8. Delivery：manifest mutation、partial bundle、exact retry、registry/route no-op；无 final model/runtime side effect。
9. 全量：targeted tests、`advisory_modeling_backend`、Ruff/format、compile/mypy、L0、ownership、F2 validator、DESIGN-COMPLIANCE-001。

## 13. Risks and controls / 风险与控制

| 风险 | 控制 |
|---|---|
| 分钟源 manifest 与 N2-A universe 不一致 | universe 只来自 N2-A；manifest 仅作 coverage，不过滤 |
| 停牌/缺分钟被误删或填零 | 所有 key 保留；typed missing + train-fold median + coverage flags |
| T+1 或未来 bar 泄漏 | timestamp cutoff、exact day projection、future poison、request identity |
| 日线可表达特征伪装成新分钟信息 | roster 聚焦时段路径、realized vol、VWAP、amount concentration、limit duration |
| 33GB 数据造成内存/时长工程化 | 单日八列流式聚合、8 GiB fail closed、wall 只记录；不建平台 |
| parent 重新拟合被误认成分钟增量 | 固定 parent-only Ridge comparator，并要求同时优于 current parent/comparator |
| 同一窗口继续 feature fishing | exact roster、两 trial、frontier 一次选择；selected=0 转上游 Alpha 生成设计 |
| 研究代码误接生产 | 无 final refit/API/runtime adapter；所有 activation/business-write flags false |

## 14. Rollout, rollback and Production gates

Rollout 仅指源码合入后从 clean main 生成一次冻结 request，并在 WSL `rdagent-gpu` 运行开发窗口审计。结果不接生产。

回滚只删除未发布临时失败目录或以新 PR 回退源码；immutable bundle 与 append-only registry 不改写。`production_ddl_gate=noop`，`backend_restart_gate=noop`，`runtime_activation_gate=noop`。

## 15. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-920 | 只从腿间正式 selected=0 进入；selected=0 后不回搜旧 feature/loss/model |
| F-921 | leg/N2-A/N1/minute snapshot 四源身份、关系和 content inventory fail closed |
| F-922 | T 日收盘时钟、八字段/八聚合/11 项 expanded exact roster，future/label poison 不变 |
| F-923 | whole-day/partial missing 不阻断、不删除、不填零，train-only median 与 coverage flags |
| F-924 | 两个 Ridge trial 诚实计数，固定 alpha/solver/preprocess，无模型搜索/final refit |
| F-925 | 精确复用 28 CPCV path，每 source row 恰好 7 OOF，仅 finite-evaluable 训练 |
| F-926 | paired parent/comparator 评价、family-wise 区间、双 baseline 支持、MDE 与一次选择 |
| F-927 | 单日流式读取、RSS<8 GiB、temp<16 GiB、wall=null、无通用平台 |
| F-928 | immutable bundle、manifest、registry、route、inspect 和 exact retry no-op |
| F-929 | sealed/DB/network/daily/runtime/factor/package/position 禁止，无 restart/DDL |
| F-930 | selected=1 只进 minute confirmation；selected=0 唯一转 QE 上游 Alpha generator MVE 设计 |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-920 | `prepare_minute_information_set_request`；`_validate_bound_sources` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/request.json`；`backend/tests/advisory_model_first/test_minute_information_set_delivery.py` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-921 | `fingerprint_minute_source`；`_validate_source_control_refs`；`_read_minute_bundle` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/source_identity_receipt.json` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-922 | `build_minute_feature_panel`；`aggregate_minute_day` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/minute_feature_panel.parquet`；`backend/tests/advisory_model_first/test_minute_information_set_pipeline.py` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-923 | `aggregate_minute_day`；`run_minute_crossfit` train-fold imputer | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/minute_coverage_daily.parquet`；`fold_diagnostics.parquet` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-924 | `FrozenMinuteInformationSetRequestV1`；`MinuteInformationSetModelTrialV1` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/request.json`；`backend/tests/advisory_model_first/test_minute_information_set_contracts.py` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-925 | `run_minute_crossfit` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/oof_score_panel.parquet` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-926 | `evaluate_minute_models` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/model_summary.json`；`frontier_receipt.json` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-927 | `_QlibMinuteLoader`；`_check_resource_limits`；`_verify_environment` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/resource_report.json` | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-928 | `prepare_minute_information_set_request`；`run_minute_information_set_mve`；`inspect_minute_information_set_bundle`；`_deliver_bundle` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/manifest.json`；exact retry readback | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-929 | frozen request flags；CLI；CI classifier mapping | `backend/tests/advisory_model_first/test_minute_information_set_contracts.py`；`backend/tests/scripts/test_ci_change_classifier.py`；PR #4210 CI/CodeQL/L0 pass | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |
| F-930 | `MinuteInformationSetReceiptV1`；`_write_route_page` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_minute_information_set_formal_v1_20260903/minute_information_set_bundles/0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9/learnability_receipt.json`；route readback | IMPLEMENTED_FORMAL_VERIFIED_SELECTED_ZERO | none |

## 17. DESIGN-COMPLIANCE-001

1. **设计目标逐项覆盖**：F-920 至 F-930 均已映射到真实源码与正式 artifact；source-ready、local verification 与正式经济结果保持独立。
2. **代码逐项映射设计**：实现保持 §11 精确范围；没有新增通用分钟平台、运行时激活、仓位或交易输出。
3. **测试逐项证明业务结果**：本地测试覆盖 PIT、真实/归一化 missing、相邻 minute return、7 OOF、parent/comparator 增量、route 与 exact retry；正式 386 日 run 对全部行、日期、支持、资源和经济指标完成 readback。
4. **差距显式保留**：本 MVE 已 formal complete 且 `selected=0`；它没有可进入 confirmation 的 candidate，也没有模型/因子/包/运行时/仓位输出。下一新假设是独立 `N3_QE_ALPHA_GENERATOR_MVE_DESIGN`，不得回选本 frontier。

## 18. Formal Result / 正式结果

- request：`advn3minreq_333ff0cf8e102efef88961c3`，clean `main@a82a58ce5ef1837a999691234f4a516d7fecf68f`。
- bundle：`0076a3a6c1e0fa40f6a29a73ab35c4015ae27431fb68989ce13fbb79e56a89f9`，receipt：`advn3minrcpt_727b08ffe123f191f619f7ba`，结果 `2/2/2/0`。
- source：1,710,301 行、386 日；raw `1,695,153 complete / 13,473 partial / 1,675 whole-day missing`，session-normalized `1,708,614 complete / 12 partial / 1,675 whole-day missing`；三个 deficit 日期精确命中。
- OOF：两个 trial 的全部 1,710,301 行均恰好 7 次；fold diagnostics 56 行；384 个 paired-evaluable 日，candidate 相对 parent/comparator 均在 384 日发生真实干预。
- 经济结果：candidate RankIC `0.090195`，parent/comparator `0.122839`，delta `-0.032643`；candidate Top5 净超额 `128.31 bps`，parent/comparator `443.65 bps`，lift `-315.32 bps`。相对 parent 的 family-wise lower 为 RankIC `-0.072894`、Top5 `-480.67 bps`；相对 comparator 为 `-0.077498/-472.57 bps`。四项门槛全部失败。
- 干预与相关：candidate-parent 日均 score Spearman `0.789179`，说明输出并非恒等；support 为 384/384 intervention days、fraction `1.0`，失败来自经济退化而非覆盖或零动作。
- 资源：elapsed `853.45s`，peak RSS `2,326,515,712 bytes`，temp `184,274,656 bytes`，均低于冻结门槛；wall time 仅 telemetry。
- delivery：首次 registry append `1`、总计 `25`；exact retry 使用相同 bundle，registry duplicate-noop、route exact-noop。sealed holdout、DB、network、Qlib daily、final model、factor、StrategyPackage、runtime、position 全为 false。
- 结论：T 日这组固定分钟聚合在当前父包候选和简单冻结 Ridge 下可改变排序，但显著降低 RankIC 与 Top5 成本后收益；本 frontier 消费并关闭，不换窗口、特征、alpha、模型族或方向。唯一 next task 为 `N3_QE_ALPHA_GENERATOR_MVE_DESIGN`。
