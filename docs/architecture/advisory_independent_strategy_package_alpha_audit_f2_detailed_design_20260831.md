# AIstock Advisory 独立 StrategyPackage 同窗 Alpha 审计 F2 详细设计 v1.6

- 日期：2026-08-31
- Feature tier：F2
- 当前状态：`V2_SOURCE_MERGE_READY_V1_ABORTED_NO_EVIDENCE_FORMAL_RUN_PENDING`
- 业务归属：Selection Center / Advisory / StrategyPackage / QE Research
- 目标合同：`ALPHA_RANKING`
- 研究类型：`ORACLE_DIAGNOSTIC`
- 证据用途：`NAVIGATION_ONLY`
- 前置依赖：N1 PR #4014 与 N2-A PR #4048 已合入；N2-A formal bundle `6784df1a...` 已完成且 sealed holdout 未读

## 1. Background / 当前事实

1. N1 固定 development window 为 386 个决策日：`2024-07-04..2026-02-02`，H20 outcome 截止 `2026-03-10`。canonical PIT 覆盖 5,067 只股票、5,077 个 eligibility span。
2. N2-A 已对当前父包的 `LSTM_ONLY`、`FUNDGROWTH_ONLY` 与 `IC_WEIGHTED_PARENT` 完成同窗归因。当前父包相对 FUND 有稳定增量，但相对 LSTM 的 RankIC 与 Top5 增量区间均跨 0；父包 Top50 全市场赢家召回只有 `1.7617%`。
3. 当前 registry 中可进入本轮的独立单 Alpha 包只剩以下两条：
   - `pkg_378eb9c91e104c64935404e257e932ee`：`BACKTEST_APPROVED`，57 因子，自定义 LSTM；
   - `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27`：`PAPER_ENABLED`，57 因子，LGB。
4. 两包均已通过 `2026-02-02` frozen package-owned CAS 单日推理可产性验证，有限 score 数均为 1,210；该事实只证明“冻结模型可推理”，不是同窗 Alpha 证据。
5. 两包的有序 `(factor_name, factor asset sha256)` 闭包完全相同，closure hash 为 `977c29e8e328d393bd8235821070e19a96bb23ef0434430c5437621261fb542c`。因此每个决策日只执行一个 exact closure，两包共享同日 feature matrix，模型仍按包分别加载一次。
6. 两包原生 label、回测窗口、执行策略、成本与候选池不同。原生 Sharpe、年化收益和 RankIC 只能作 inventory，不得替代共同 N1 window/PIT/H20/cost 结果。
7. 本任务消费已用过的 development window，只作导航诊断；不读取 sealed holdout、不训练或调参、不改变 Selection、StrategyPackage、Advisory、Paper 或生产运行时。
8. 实现期 package-owned 源码审核确认两包均包含按 `datetime` 横截面 rank/z-score/波动聚合的因子。把 386 日 instrument union 一次性计算后再做每日 PIT 过滤，会改变这些因子的横截面支持集，不能与“在历史 T 按 T 的 canonical PIT 股票池运行单日业务逻辑”等价。v1.2 因此冻结为一次区间读取、单进程内 386 个 PIT 日批处理、每个 T 一个 closure；禁止退回 union-wide factor matrix。
9. v1.2 首次真实运行显示 file-backed wrapper 约需 8 分钟/PIT 日，线性外推明显超过 8 小时。复审同时发现其后续 T 使用从首个 warm-up 日起不断扩张的输入，而单日业务语义是每个 T 独立使用该 closure 的 `required_window + 5` 滚动交易日窗口；扩张窗口既浪费计算，也可能改变 expanding/分位类结果，因此旧运行不得形成正式证据。v1.3 固定为一次完整区间读取后按 T/closure 切出滚动窗口、在该窗口内只预计算一次 static 派生，再使用受限 in-memory I/O adapter。adapter 只虚拟化已知 `daily_pv/static aliases/result.h5` 的 pandas read/write，冻结因子源码、输入值、执行顺序和返回值不变；必须用 file-backed reference 做逐值 parity，遇到 HDFStore/h5py/外部 reader 即 fail closed。
10. v1.3/v1.4 针对原三包双 closure 的真实性与性能实验仍只作为实现诊断：真实 file-backed parity、COW 输入隔离、同源 target 复用与 WSL local `TemporaryDirectory` 均已验证。v2 roster 只保留共同的 57 因子 closure，不再执行、复用或计数已退役包的 50 因子 closure。
11. v1.5 已证明只在 factor 完整计算结束后、虚拟 `result.h5` 物化之前投影 request 明列 decision date，可与真实 file-backed 全量物化后截取 T 保持逐值、dtype、index、column 和 hash 完全一致。v2 保留该优化和 fail-closed parity，不改变 factor 源码、输入窗口、计算过程或 T 行结果。
12. 2026-09-01 用户明确取消实验的 8 小时终止门禁：新冻结 request 只能是 `resource_max_wall_seconds=null`；总墙钟写入 telemetry/receipt，但不参与成功判定。RSS 16 GiB、临时空间 32 GiB、PIT、文件 parity、因果性和输出完整性门禁不变。运行进度每 30 分钟轻量检查。
13. v1 frozen request `advpkgareq_ef3d0abb...` 运行约 6 小时后因第三包的 `neg_vol_adjusted_momentum` 未来追加不变性违规而退出，未发布 bundle、未追加 registry、未形成研究证据。BUG-1302 已将该因子隔离并把 `pkg_b668f8a633c44b72a5d557a2cb8970e3` 退役；替代 `neg_vol_adjusted_momentum_pit_v2` 虽 PIT 正确但正式评级 D 且保持禁用。v2 roster 的删减完全由独立 PIT 安全事件决定，不使用审计收益结果；request 必须绑定已验证 BUG-1302 receipt、被排除包 id/status/manifest 与因子名，禁止把旧 request 改写后续跑。

## 2. Goal and scope

交付一个边界固定的 N2-B 离线审计闭环：

1. 固定三个且仅三个审计 arm：
   - `CURRENT_IC_PARENT`：N2-A formal bundle 中的当前父包；
   - `PKG_378EB9`：`pkg_378eb9c91e104c64935404e257e932ee`；
   - `PKG_5A5CCB`：`pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27`。
2. 按上述旧包顺序，从 package-owned CAS 冻结 manifest、模型、模型代码和因子代码；不回退旧 QE workspace、source id、网络节点或缓存模型。
3. 一次读取完整历史行情/基本面/资金流/行业区间；在同一个 WSL 进程中按 386 个 T 的 frozen PIT 股票池切分已读内存 panel，每个 T 执行一个 exact factor closure，共 386 次 primary closure evaluation；两包共享同日 closure matrix，每个模型只反序列化一次。它是一次批量任务，不创建逐日进程、DB 查询、workspace 或模型加载。固定 future-poison/isolated 诊断复用同一内存输入和已加载模型，单独计数。
4. 为两个旧包发布 task-local、content-addressed Prediction Store artifact；每个 artifact 独立报告日期、PIT、有效 score、缺失特征和 source receipt。
5. 复用 N1 canonical PIT、full-universe H20 outcome、benchmark、停牌/涨跌停、成本与容量折损，比较每个 arm 自身覆盖下的 signal、Top5、Top20/40/50 winner recall 与 oracle headroom。
6. 所有 arm 同时报告自身 universe 指标；pairwise score/RankIC 只在对应两臂共同 instrument-date 上计算。禁止用三臂总 intersection 掩盖旧包覆盖瓶颈。
7. 生成 immutable bundle、exact retry、资源/因果/parity receipt，并向 trial registry 追加一条 0-trial navigation record；route 继续保持 N2，不提前选择 N3 winner。

## 3. Non-goals

- 不训练、微调、重训或选择模型、seed、特征、package、窗口、阈值、权重或候选深度。
- 不把两个旧包当作两次模型 trial；它们是预冻结 inventory arm，registry trial 数仍为 0。
- 不读取、探测或消费 sealed holdout；不以季度、regime 或最好旧包关闭研究方向或支持激活。
- 不引入其他 `RETIRED` 包，不恢复 BUG-1302 退役包身份，不从旧原生指标事后增删 arm。
- 不把 N2-A 的 LSTM/FUND 腿重复列为 StrategyPackage arm；它们只保留为已完成归因上下文。
- 不为批量研究写业务 DB，不写 Selection artifact repository，不修改 package lifecycle、active binding、descriptor、Program、Paper、QMT 或前端/API。
- 不建设通用 batch 平台、scheduler、缓存服务、UI、审批流、ModelOps 或历史归档系统。
- 不执行后端启动/停止/重启、DDL、DML、依赖安装或运行时激活。

## 4. Architecture

```text
N0 window + N1 request/bundle + N2-A formal bundle
                         |
              sealed access deny / development authorize
                         |
      frozen package snapshots + package-owned CAS workspaces
                         |
       one bounded market/static/sector interval read (read-only)
                         |
                          |
                  factor closure 977c...
                  one run per PIT day
                  386 runs, same process
                          |
                 model load once: pkg378
                 model load once: pkg5a5
                          |
         two content-addressed Prediction Store artifacts
                          |
      PIT-bound input/readback + exact Top50 per package
                          |
 CURRENT_IC_PARENT from immutable N2-A + two package arms
                          |
       one shared full-universe H20 outcome construction
                          |
 own-universe metrics + pairwise-common metrics + fixed deltas
                          |
 immutable N2-B bundle + 0-trial registry row + unchanged route
```

代码边界：

- `independent_package_alpha_audit_contracts.py`：request、package snapshot、workspace descriptor、receipt 与 manifest schema；
- `strategy_package_batch_prediction.py`：只读区间输入、factor closure grouping、一次模型加载、批量预测、PIT 过滤与因果/parity 诊断；
- `independent_package_alpha_audit_pipeline.py`：N1/N2-A source verification、三臂 metrics、bundle、exact retry、registry 与 route；
- CLI：只接受显式 request/bundle 路径；不扫描 latest、不访问 sealed、不控制服务。

## 5. Frozen request contract

### 5.1 Frozen request schema v2

request 必须冻结：

- N0 completion/window、N1 request/formal bundle 与 N2-A request/formal bundle的 role、URI、SHA256、size 和 semantic identity；
- `window_id=P0C_DEVELOPMENT_V1`、`decision_date_start=2024-07-04`、`decision_date_end=2026-02-02`、`data_cutoff=2026-03-10`；
- canonical PIT snapshot、calendar、daily Qlib outcome source、suspend、benchmark、H20、cost、capacity、baseline/shadow policy identity；
- 固定三臂 roster 与两包固定执行顺序；
- 每包 package id、status、manifest SHA256、alpha mode、model/factor/runtime asset closure、workspace file descriptors；
- 一个 57 因子 closure group，group member 只能由 exact ordered factor asset closure 相等推导，不能按因子数量或名称相似合并；
- BUG-1302 已验证 receipt 的 role/URI/SHA256/size，以及被排除包的 id、`RETIRED` 状态、manifest SHA256 和违规因子名；该排除依据不得替换为研究收益结果；
- Prediction Store root、output root、registry path、repository clean commit；
- bootstrap 20 个交易日 block、2,000 repetitions、seed `20260831`；
- 因果 anchor 固定为 `2024-07-04`、`2025-04-22`、`2026-02-02`；
- `resource_max_rss_bytes=17179869184`、`resource_max_wall_seconds=null`、`resource_max_temp_bytes=34359738368`；墙钟只记录、不自动终止。

request 使用 canonical JSON 形成 `request_id`。unknown field、package 顺序/状态/manifest/closure 漂移、重复包、额外 arm、未绑定的 roster 排除、BUG-1302 receipt 漂移、N1/N2-A identity 漂移、sealed 日期或非 clean commit 一律拒绝。

### 5.2 Package source freeze

`prepare` 在 Windows task worktree 中通过 `StrategyPackageRepository` 读取当前 record，随后只调用：

```text
QEExperimentRuntimeAssetResolver.load_frozen_source_for_strategy_package(...)
QEExperimentRuntimeAssetResolver.prepare_workspace(...)
```

约束：

- `load_source_for_strategy_package`、旧 QE source lookup、node fetch 与 worker workspace fallback 在本任务中禁止；
- package snapshot 保存完整 canonical manifest；workspace 每个文件记录相对路径、SHA256 与 size；
- 自定义模型代码与 pickle 依赖必须包含在 package closure 中；缺失即 typed failure；
- 正式 WSL run 只读冻结 snapshot/workspace，不重新查询 package 状态或资产；
- task-local workspace 与 Prediction Store 位于显式 external artifact root，不进入源码或生产 runtime cache。

## 6. Batch prediction contract

### 6.1 One interval read, exact PIT-day factor batches, two model loads

1. 从 frozen PIT spans 得到 386 日所需 instrument union；从 trading calendar 将最早决策日向前扩展 `max_required_window + 5` 个交易日。
2. 在 WSL `rdagent-gpu` 中，对 union 和完整区间各执行一次只读来源加载：
   - 日线 OHLCVA/factor read；
   - 基本面、资金流与行业 read。
3. 外部行情 fallback、Tushare fallback、Selection data cache、逐日 DB query 均关闭；receipt 记录 query contract、范围、row count 与内容 hash。
4. 正式 batch 默认使用受限 in-memory pandas I/O adapter 提供 canonical daily/static DataFrame；完整区间只 canonicalize 一次，每个 T 按 closure 的 `required_window + 5` 个交易日切片，随后在已切片的 PIT 股票池内只计算一次 static 派生。每个 factor read 返回 pandas copy-on-write 隔离视图，禁止共享可写深层状态。factor 完成全部计算后，虚拟 `result.h5` 只保存 request 明列 decision date 的 copy-on-write 快照，禁止提前裁剪 factor 输入或中间计算；投影前必须在全量 result 上保持原有 datetime/instrument 规范化与重复键校验语义，任何需要规范化或无法证明安全的 index 都保留全量 result 并标记 `FULL_RESULT_SEMANTIC_FALLBACK`，正式审计据此 fail closed。file-backed reference 仍走完整文件物化并在 consumer 侧截取同一日期。首日真实 file-backed parity 是等价硬门禁。临时计算必须位于 WSL 环境本地 `TemporaryDirectory`，不得在 `/mnt/f` artifact root 做逐因子临时元数据往返；最终 request、Prediction Store、bundle 与 receipt 仍只写 AIstock artifact root。
5. 对每个 decision T，先按该日 canonical PIT member set 和冻结滚动窗口从已读 panel 切出业务等价输入，再运行 exact factor closure `977c...`；两包共享同日 factor matrix 只因资产闭包精确相同。完整窗口固定为 `386 × 1 = 386` 次 primary closure evaluation。首个冻结 T 对真实 closure 追加一次不消费缓存的 `FILE_BACKED_REFERENCE`，与同输入内存矩阵逐 key、column、dtype、value 精确比较；该 reference 只是实现等价门禁，不计研究 trial。
6. 每个 package model 只调用一次 `load_model_from_pkl`；模型对象跨 386 日复用，每日矩阵使用同一冻结 infer processor 和 `predict`，不重新加载模型、来源、CAS 或 workspace。
7. 正式 receipt 必须满足：`market_interval_read_count=1`、`static_interval_read_count=1`、`rolling_live_window_semantics=true`、逐 closure `required_window_by_closure`、`window_buffer_trading_days=5`、`factor_io_mode=IN_MEMORY_EQUIVALENT`、`factor_input_copy_mode=PANDAS_COPY_ON_WRITE`、`factor_result_projection_mode=DECISION_DATES_BEFORE_MATERIALIZATION`、`wall_limit_enabled=false`、`wall_limit_seconds=null`、`temp_storage_mode=ENVIRONMENT_LOCAL_EPHEMERAL`、`primary_decision_batch_count=386`、`primary_factor_group_run_count_per_decision=1`、`primary_factor_group_run_count=386`、`diagnostic_factor_group_run_count=3`、`factor_group_total_run_count=389`、`file_backed_parity_factor_group_run_count=1`、`all_factor_group_run_count=390`、`factor_calculation_count=22173`、`factor_reuse_count=0`、`result_write_count=22173`、`projected_result_write_count=22173`、`fallback_result_write_count=0`、`reference_factor_calculation_count=57`、每包 `model_load_count=1`、`daily_wsl_process_count=0`、`daily_db_query_count=0`。唯一 parity receipt 的 closure 与 feature hash 必须一致。诊断重算只切已读内存 panel，保持与 primary 相同下界并只为 poison 扩展上界，不再次访问 DB、CAS 或加载模型；总墙钟只写 telemetry，不形成自动终止或验收阈值。

### 6.2 PIT and missing-data semantics

- score 日期为 decision T；完整输入最大日期不得超过 `2026-02-02`，outcome 只在排名完成后读取至 `2026-03-10`。
- 每个 T 必须先按 canonical PIT span 冻结 factor 输入股票池，再生成冻结模型原始 score；score 输出再做同一 PIT key readback。上市不足 252 个交易日、ST/退市状态遵循 N1 snapshot。禁止 union-wide 横截面计算后置过滤。
- 每日记录 PIT member count、feature input count、finite score count、dropped row count、invalid feature columns/count 与 Top50 status。
- 缺失特征不得填 0、均值或跨包 score；保留 `UNSCORABLE_FEATURE_MISSING` coverage。某只股票缺失不阻断整个日期；某日少于 50 个 finite score 则该 arm 当日 `DATA_UNAVAILABLE`，不得用其他包补位。
- 停牌、一字涨跌停、行情缺失和退出不可执行仍由 N1 full-universe outcome typed status 处理，不删除股票或中断全任务。

### 6.3 Causality and parity

未来泄漏与批量语义使用同一个已加载模型检查：

1. primary PIT-day batch 对三个固定 anchor 留存 score/rank；
2. 对前两个 anchor，使用相同 anchor PIT 股票池但向输入注入 anchor 后已读行做 future-poison 重算；primary 与 poisoned 结果必须保持 instrument key、Top50 和标准化 score parity。若 factor 偷看未来行即失败；
3. 三个 anchor 合计形成每个 closure 三次有界诊断重算，不产生新的研究 arm/trial；前两个是 future-poison，`2026-02-02` 是 isolated end-date exact retry；
4. `2026-02-02` 使用该日 frozen PIT instrument set 重算，要求 Top50 exact parity、Spearman `>=0.999999`；
5. 任何 anchor 失败即 `ADVISORY_PACKAGE_BATCH_FUTURE_DEPENDENCY_DETECTED` 或 `ADVISORY_PACKAGE_BATCH_LIVE_PARITY_FAILED`，不得发布 Prediction Store 或 Alpha 结果。

静态扫描同时拒绝本批次 factor closure 中明确的 future operator，例如负 shift、centered rolling、反向切片后前向填充等；扫描是输入校验，不替代 prefix poison test。

### 6.4 Prediction Store

每包 run key 固定为 request identity + package identity。artifact 为 Qlib-compatible `pred.pkl`：

- MultiIndex 必须精确为 `(datetime, instrument)`；
- 唯一列为 finite `score`；
- 日期只能是 386 个 development decision date；
- 重复键、未知日期、PIT 外股票、空日期或非有限 score 拒绝；
- manifest 记录 package/manifest/model/factor closure、source receipts、row/symbol/date coverage 与 causal/parity receipt；
- store 位于本任务 output root，禁止覆盖既有不同内容 run；exact retry 直接返回 immutable audit bundle，不重写 store manifest。

## 7. Three-arm common audit

### 7.1 Arm-specific universe

`CURRENT_IC_PARENT` 不重新生成：它必须从 N2-A formal `full_universe_signal_outcomes.parquet` 读取 `score__IC_WEIGHTED_PARENT`，并与 N2-A `arm_rankings_top50.parquet` 的父包 Top50 精确 parity。两个旧包来自本任务 Prediction Store。

每个 arm 在自身 PIT-valid prediction universe 上独立计算：

1. matured Pearson IC、matured Spearman RankIC 与 policy-known RankIC；
2. daily mean/median/std、ICIR、positive-day fraction、20 日 moving-block CI；
3. quintile/decile bucket return 与 top-minus-bottom spread；
4. exact Top20/40/50 对全市场 H20 Top5 winner recall、random expected recall 与 lift；
5. Top5 fixed-slot 成本后超额、正收益日比例、累计 episode mean；
6. Top20 内 perfect Top5 hindsight headroom 与实际 intervention support；
7. prediction/PIT/known/matured/Top50 coverage。

random recall 的抽样支持为该 arm 当日 finite PIT-valid prediction universe，不能直接用 `depth / PIT member count`，也不能把低覆盖旧包的可选域冒充全市场。

### 7.2 Pairwise comparison

固定三对，不事后删选：

- 两个旧包分别与 `CURRENT_IC_PARENT` 比较；
- `PKG_378EB9 - PKG_5A5CCB`。

每对报告：

- matched-day Top5 net excess delta 与 block CI；
- 在该 pair 的共同 instrument-date 上重算 daily RankIC 后形成 delta 与 block CI；不得直接相减两个 own-universe RankIC；
- 两臂共同 instrument-date 上的 raw/normalized score Pearson、Spearman 与 residual correlation；
- Top5/Top20 Jaccard、rank overlap、各自 churn；
- own-universe coverage 与 pairwise-common coverage，禁止把 pairwise intersection 指标冒充 arm 主结果。

本轮不选 winner、不调组合权重。若某旧包优于当前父包，结论仍只是“值得进入 N3 候选来源分流”；confirmation 与 activation 必须使用新 lineage 和未消费证据。

### 7.3 Regime and sample interval

- 主结论固定完整 386 日；
- regime 沿用 N1 `CSI300_TRAILING20_CLOSE_RETURN_SIGN_AT_T_V1`，只使用 T 可见信息；
- quarter 只作描述性 sensitivity，不用于选包；
- 所有正式区间使用 20 日 moving-block bootstrap；
- 两条存续旧包的原生回测指标只进入 `inventory_context.json`，明确 `NOT_COMPARABLE_TO_COMMON_AUDIT`；被排除包只记录独立 PIT 安全事件和退役身份，不进入收益 inventory。

## 8. Outcome and evidence boundary

1. `prepare` 只校验显式 N1 development contract并冻结 package metadata/CAS workspace；它不读取 Prediction Store、PIT members、行情、factor values、outcome 或 sealed 数据。正式 `run` 的 `authorize_n1_development_access` 必须发生在读取冻结 workspace 以及任何 Prediction Store、Qlib、PIT、factor、market 或 suspend scientific loader 之前。
2. full-universe H20 outcome 复用 N1 helper，T+1 open 入场、T+20 planned open 退出、最多顺延 5 个交易日、benchmark 实际日期、buy `0.95bps`、sell `5.95bps` 与额外 `5bps` capacity haircut 全部不变。
3. 排名只能读取 T 及以前的冻结 score；outcome 在 score 与 Top50 完成后 join。
4. sealed window id、consume receipt、sealed path 或超出 development contract 的日期立即失败。
5. bundle 固定 `sealed_holdout_accessed=false`、`runtime_eligible=false`、`activated=false`。

## 9. Artifacts and exact retry

Bundle 至少包含：

```text
request.json
source_identity_receipt.json
package_inventory_context.json
batch_prediction_receipt.json
causality_parity_receipt.json
prediction_descriptors.json
coverage_daily.parquet
arm_signal_outcomes.parquet
arm_rankings_top50.parquet
arm_recall_daily.parquet
arm_top5_daily.parquet
arm_oracle_daily.parquet
signal_metrics_daily.parquet
arm_summary.json
pairwise_summary.json
regime_quarter_summary.parquet
audit_receipt.json
registry_record.json
resource_report.json
environment.json
manifest.json
```

- 科学结果 identity 排除 `created_at`、墙钟和 RSS 波动；request/source/result semantic hashes 全量绑定。
- 临时目录 fsync 后原子发布到 content-addressed bundle；同 request 多 bundle、同 run key 不同内容或 readback hash 不一致均失败。
- bundle readback 完整后才 append registry；exact retry 返回 `EXISTING_BUNDLE`、registry duplicate no-op，Prediction Store 不重写。
- registry 固定：

```text
study_type=ORACLE_DIAGNOSTIC
objective_contract=ALPHA_RANKING
planned/generated/evaluated/selected_trial_count=0
result_class=EXPLORATORY
decision_use=NAVIGATION_ONLY
```

- route 刷新后 `next_task` 仍为 `N2_ENTRY_EXIT_QE_PREPARATION`；N2-B 结果只补齐候选来源诊断，不自行激活 N3。

## 10. Error contract

| reason_code | 含义 |
|---|---|
| `ADVISORY_PACKAGE_ALPHA_AUDIT_REQUEST_INVALID` | request、roster、hash、日期、资源或 clean commit 非法 |
| `ADVISORY_PACKAGE_ALPHA_AUDIT_WINDOW_FORBIDDEN` | 非 development window、sealed 或越界读取 |
| `ADVISORY_PACKAGE_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH` | N0/N1/N2-A/package/workspace/PIT/policy identity 漂移 |
| `ADVISORY_PACKAGE_ALPHA_AUDIT_ROSTER_INVALID` | 包缺失、重复、退役、顺序/状态/manifest 不符或额外 arm |
| `ADVISORY_PACKAGE_BATCH_ASSET_INVALID` | package-owned model/code/factor/runtime closure 缺失或 hash 不符 |
| `ADVISORY_PACKAGE_BATCH_SOURCE_READ_FAILED` | 有界只读行情/基本面/资金流/行业区间不可读 |
| `ADVISORY_PACKAGE_BATCH_PREDICTION_INVALID` | 日期、PIT、重复、finite score、Top50 或 Prediction Store 非法 |
| `ADVISORY_PACKAGE_BATCH_FUTURE_DEPENDENCY_DETECTED` | prefix poison 与 full batch 不一致 |
| `ADVISORY_PACKAGE_BATCH_LIVE_PARITY_FAILED` | isolated end-date 与 batch Top50/score parity 不满足 |
| `ADVISORY_PACKAGE_ALPHA_AUDIT_OUTCOME_INVALID` | N1 H20/PIT/benchmark/suspend/cost outcome 漂移 |
| `ADVISORY_PACKAGE_ALPHA_AUDIT_BUNDLE_CONFLICT` | immutable bundle/store 冲突或 readback 失败 |
| `ADVISORY_PACKAGE_ALPHA_AUDIT_RESOURCE_LIMIT_EXCEEDED` | 仅用于 RSS 超 16GB 或临时空间超 32GB；墙钟不再触发该错误 |
| `ADVISORY_MODEL_TRAINING_REQUIRES_WSL` | 正式 run 不在 WSL `rdagent-gpu` |

## 11. Implementation Plan / scope and phases

允许修改：

```text
backend/services/advisory_model_first/independent_package_alpha_audit_contracts.py
backend/services/advisory_model_first/strategy_package_batch_prediction.py
backend/services/advisory_model_first/independent_package_alpha_audit_pipeline.py
backend/tests/advisory_model_first/test_independent_package_alpha_audit_contracts.py
backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py
backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py
scripts/advisory_independent_package_alpha_audit.py
scripts/ci_change_classifier.py
backend/tests/scripts/test_ci_change_classifier.py
tests/aistock_validation/catalog/file_ownership.yaml
docs/architecture/advisory_independent_strategy_package_alpha_audit_f2_detailed_design_20260831.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

若必须修改 `backend/inference_engine.py` 或 StrategyPackage 生产路径，先回到设计审核；默认实现只复用其纯函数，不改生产推理行为。

阶段：

1. 合同、roster、source/workspace freeze 与 sealed-first 拒绝测试；
2. 一次区间读取、factor closure grouping、每包一次模型加载与 Prediction Store；
3. future poison、isolated parity、PIT/缺失 coverage；
4. 三臂 own-universe metrics、三组 pairwise、regime/quarter；
5. immutable bundle、exact retry、registry/route；
6. CLI、classifier、ownership、F2 acceptance；
7. 多轮代码/设计审核修复；
8. 绑定 clean compute commit 后执行正式 WSL 审计。

## 12. Verification Plan

### 12.1 Contracts / source

- 精确两包顺序、三 arm、manifest/status/closure 与 BUG-1302 roster 排除绑定正负测试；
- 非 RETIRED 排除包、额外/重复包、unknown field、dirty commit、workspace file drift 拒绝；
- package-owned CAS-only，旧 QE source/node/fallback 调用即测试失败；
- sealed authorization 在所有数据 loader 前执行。

### 12.2 Batch / causality / PIT

- fake loader 证明一次 market/static read、386 个 PIT 日 × 一个 primary closure、三次固定诊断重算、每包一次 model load、零逐日 WSL process/DB query/workspace rebuild；
- 同一微型 frozen factor 分别以 `FILE_BACKED_REFERENCE` 与 `IN_MEMORY_EQUIVALENT` 运行，keys、columns、dtype 与 values 精确一致；虚拟 result 只在 factor 完整计算后投影 request decision date，DataFrame/Series result 与 clean-daily 的 COW 快照在调用方随后修改时仍保持写入时值；原 pandas I/O/COW option 在异常和成功路径都恢复；正式 batch 首个 T 对唯一真实 closure 重复 parity 并固化 hash；fake batch 证明每个 T 使用固定 `required_window + 5` 日而非扩张窗口；
- 相同 factor count 但不同 asset hash 不得共享；相同 closure 必须共享；
- 三个固定 anchor 的 full/prefix poison 正负测试；
- isolated end-date exact Top50 与 score tolerance 正负测试；
- 上市 252 session、ST/退市、缺失特征、停牌、涨跌停与行情缺失均保留 typed semantics；
- future outcome poison 不改变任何 score/ranking/prediction artifact。

### 12.3 Metrics

- 三臂各自 coverage，不得强制全局 intersection；
- pairwise score 只用两臂 common keys，Top5 delta 用 matched days；
- full-market winner、random support、Top20/40/50、Top5、oracle、bucket、block CI；
- N2-A parent score/Top50 与 formal bundle parity；
- regime at T、quarter descriptive only、固定三对比较。

### 12.4 Delivery

- bundle atomic publish、content hash/readback、冲突、exact retry；
- registry 0-trial append/duplicate no-op，route 不变；
- peak RSS、peak temp bytes、wall time、model/data/factor call counts 与 H5 hard-link identity 写入正式 receipt；
- F2 validator、定向 pytest、Ruff、format、compile、ownership、classifier、L0、`git diff --check`；
- `DESIGN-COMPLIANCE-001` 四项逐项复核后才可报告 PR/merge ready。

## 13. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-221 | 固定当前父包与两条仍有效的独立旧包；顺序、状态、manifest 与共同 closure 预冻结；第三包仅因 BUG-1302 PIT 安全事件排除且 receipt 全量绑定 |
| F-222 | N0/N1/N2-A/window/PIT/policy/outcome identity 全量绑定，sealed-first fail closed |
| F-223 | package-owned CAS-only workspace，不回退 QE source、node、worker workspace 或缓存模型 |
| F-224 | 一次完整区间 market/static read；单进程内 386 个 PIT 日 × 一个 exact closure；三次固定内存诊断重算；每包模型只加载一次；零逐日 WSL process/DB query/workspace rebuild |
| F-225 | 三个固定 prefix poison anchor 与 isolated end-date parity 阻止 batch future leakage/语义漂移 |
| F-226 | Prediction Store artifact 为 386 日、PIT-valid、finite、唯一键且 content-addressed |
| F-227 | 缺失特征、低覆盖、停牌、涨跌停、行情缺失保留 typed coverage/outcome，不填值、不补包 |
| F-228 | 三臂主指标各用 own universe；pairwise 只用两臂 common keys，不用全局 intersection 掩盖覆盖 |
| F-229 | 每臂完整报告 IC/RankIC、bucket、Top5、Top20/40/50 recall、random lift 与 oracle |
| F-230 | 固定三组 pairwise delta、相关、overlap 与 churn，不事后选包或比较对 |
| F-231 | 完整窗口为主结论，T 可见 regime 与 quarter sensitivity 前置冻结并仅描述 |
| F-232 | N1 full-universe H20/PIT/benchmark/suspend/cost 复用且 outcome 只在 ranking 后 join |
| F-233 | immutable bundle、Prediction Store descriptors、atomic publish、readback、exact retry |
| F-234 | registry 为 0-trial ORACLE_DIAGNOSTIC/NAVIGATION_ONLY，route 保持 N2 |
| F-235 | WSL `rdagent-gpu` 正式运行，RSS <16GB、temp <32GB；wall 仅记录且不自动终止，资源/调用/H5链接次数可读回 |
| F-236 | 无 API/UI/DB 写入/restart/runtime/Selection/StrategyPackage lifecycle 影响 |
| F-237 | 仅在 factor 完整计算结束后把虚拟 result 投影到 request decision date；COW 快照与真实 file-backed 全量物化后截取结果完全一致 |

## 14. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-221 | `independent_package_alpha_audit_contracts.py`; `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_contracts.py` roster cases | IMPLEMENTED_LOCAL_READY | none |
| F-222 | `independent_package_alpha_audit_contracts.py`; `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` sealed-first/source identity cases | IMPLEMENTED_LOCAL_READY | none |
| F-223 | `strategy_package_batch_prediction.py` | `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` CAS-only/fallback rejection cases | IMPLEMENTED_LOCAL_READY | none |
| F-224 | `strategy_package_batch_prediction.py` | `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` interval/factor/model call-count cases | IMPLEMENTED_LOCAL_READY | none |
| F-225 | `strategy_package_batch_prediction.py` | `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` prefix poison and isolated parity cases | IMPLEMENTED_LOCAL_READY | none |
| F-226 | `strategy_package_batch_prediction.py`; `PredictionArtifactStore` | `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` Prediction Store schema/hash cases | IMPLEMENTED_LOCAL_READY | none |
| F-227 | `strategy_package_batch_prediction.py`; `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` missing/coverage cases | IMPLEMENTED_LOCAL_READY | none |
| F-228 | `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` own/common universe cases | IMPLEMENTED_LOCAL_READY | none |
| F-229 | `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` metric cases | IMPLEMENTED_LOCAL_READY | none |
| F-230 | `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` fixed three-pair cases | IMPLEMENTED_LOCAL_READY | none |
| F-231 | `independent_package_alpha_audit_contracts.py`; `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` regime/quarter/block cases | IMPLEMENTED_LOCAL_READY | none |
| F-232 | `tier1_oracle_pipeline.py`; `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` N1/N2-A parity and outcome poison cases | IMPLEMENTED_LOCAL_READY | none |
| F-233 | `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py` bundle/exact retry/readback cases | IMPLEMENTED_LOCAL_READY | none |
| F-234 | `independent_package_alpha_audit_pipeline.py`; `research_control.py` | `backend/tests/advisory_model_first/test_research_trial_registry.py` plus N2-B route duplicate cases | IMPLEMENTED_LOCAL_READY | none |
| F-235 | `scripts/advisory_independent_package_alpha_audit.py`; pipeline resource receipt | `backend/tests/advisory_model_first/test_independent_package_alpha_audit_pipeline.py`; `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` no-wall-stop/resource/call-count/hard-link cases | IMPLEMENTED_LOCAL_READY | none |
| F-236 | no production surface; classifier/ownership metadata | `backend/tests/scripts/test_ci_change_classifier.py`; `tests/aistock_validation/catalog/file_ownership.yaml` | IMPLEMENTED_LOCAL_READY | none |
| F-237 | `strategy_package_batch_prediction.py`; `independent_package_alpha_audit_pipeline.py` | `backend/tests/advisory_model_first/test_strategy_package_batch_prediction.py` COW snapshot/projection/file-backed parity cases | IMPLEMENTED_LOCAL_READY | none |

## 15. Risks and controls

| 风险 | 影响 | 控制 |
|---|---|---|
| 原生指标口径混入 | 虚假 package 排名 | 只用共同 N1 window/PIT/H20/cost；原生指标标记不可比 |
| 全局 intersection 过窄 | 隐藏旧包覆盖不足 | own-universe 为主，pairwise common 单列报告 |
| 批量一次看到未来行 | 因子未来泄漏 | 固定 prefix poison + future operator scan + outcome poison |
| union universe 改变横截面因子单日语义 | batch/live 漂移和伪 Alpha | 每个 T 先切 T 的 PIT 股票池再算一个 closure；禁止 union-wide factor matrix；三 anchor future-poison/isolated parity |
| 相同因子数被错误复用 | 信号身份串包 | 仅 exact ordered factor asset closure 相同才共享 |
| package source 已失效 | 偷偷回退旧 QE workspace | package-owned CAS-only；所有 fallback 禁止 |
| 缺失特征被静默填充 | 虚高覆盖与收益 | 不填值，typed dropped coverage，Top50 不足当日 unavailable |
| 386 日退化为 386 个独立任务 | 重复 DB/进程/模型/workspace，浪费资源且难以恢复 | 单个 batch 进程、一次区间读、两模型各加载一次；仅 PIT-day factor evaluation 为按日，因为横截面业务语义要求如此；墙钟仅记录 |
| 从首日 warm-up 起扩张输入 | 后续日偏离单日业务窗口且计算量持续增长 | 每个 T/closure 使用冻结 `required_window + 5` 滚动交易日窗口；static 派生在切片后计算；receipt 固化窗口语义 |
| file-backed 因子 result/H5 往返导致约 8 分钟/日 | 产生无业务价值的 TB 级瞬时 I/O | 受限 in-memory pandas I/O adapter + file-backed parity；源码/输入/输出不变；未知 I/O API fail closed；不以墙钟终止替代正确性修复 |
| 深复制与 `/mnt/f` 临时元数据放大单 closure 成本 | 运行时间和临时资源被非业务开销放大 | COW 输入隔离 + WSL local ephemeral temp + decision-date result projection；首日真实 file-backed parity 和精确计数 fail closed |
| 全窗口 factor result 在只消费 T 行前仍被深复制和全量规范化 | 额外耗时和内存复制无业务价值 | 只在 factor 计算完成后的虚拟 result 物化边界投影 request decision date；file-backed reference 保持全量物化，逐值/dtype/index/hash parity fail closed |
| 诊断变成 winner 选择 | 多重检验与方向漂移 | 0 trial、navigation only、不选权重/包、不进 sealed |
| artifacts 变成新平台 | 延误模型演进 | task-local store + 一个 CLI + JSON/Parquet；无 DB/UI/scheduler |

## 16. Rollout / rollback

- rollout 只新增离线 research source、task-local artifacts 和文档；不触碰生产运行时。
- 正式结果只能在 source commit、WSL environment、request、Prediction Store 与 bundle 全部 readback 后称为 N2-B navigation evidence。
- rollback 为 revert source PR；不可变 bundle 与 registry 记录保留，以新 lineage 纠错，不覆盖旧结果。
- 本任务不需要后端重启；即使源码合入也不改变已运行服务的业务行为。

## 17. Production gates

```text
backend_restart = noop
production_ddl_gate = noop
production_dml_gate = noop
dependency_install = noop
runtime_activation = noop
client_install = noop
```

## 18. DESIGN-COMPLIANCE-001

1. 禁止简化交付：缺任一仍有效固定包、BUG-1302 roster 排除绑定、批量一次读取/一次模型加载证据、Prediction Store、PIT/因果/parity、own-universe 指标、三组 pairwise 或真实 bundle，不得称 N2-B v2 完成。
2. 禁止静默错误：资产/日期/PIT/重复/非有限 score、缺失特征、Top50 不足、future poison、identity drift 与资源超限全部 typed 失败或显式 coverage，不返回伪零值。
3. 禁止改变业务逻辑：不改 package、模型、因子、PIT、H20、成本、Selection、Advisory、Paper 或运行时；batch 只改变研究计算形态。
4. 禁止私增门禁：结果只用于导航，不新增人工审批、生产激活阈值或方向关闭规则；不确定和负结果都是合法输出。

2026-09-01 v1.6 修订 checkpoint：v1 request 因第三包独立 PIT 安全缺陷退出且未形成 bundle/registry 证据；BUG-1302 已验证关闭。v2 仅按该外生安全事件删除第三包，绑定关闭 receipt，并把 request/bundle/receipt/experiment、arm、closure、pairwise 与资源计数改为新的预注册身份。`IMPLEMENTED_LOCAL_READY`只表示对应源码与可自动验证的17项设计验收已就绪；源码合入与正式 bundle/registry/resource readback仍是两个独立状态，后者完成前不得报告 N2-B v2 navigation complete。
