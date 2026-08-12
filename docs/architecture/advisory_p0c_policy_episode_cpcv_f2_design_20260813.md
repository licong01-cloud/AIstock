# Advisory P0-C Policy Episode 标签与 CPCV/PBO 详细设计

> 日期：2026-08-13  
> Feature tier：F2  
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.0  
> 当前阶段：`SOURCE_AND_REAL_WSL_DATASET_VERIFIED_PENDING_MERGE`  
> 适用范围：学术研究与模拟荐股观察，不构成实时投资建议，不连接实盘交易或下单执行

## 1. Background / 当前真实基线

当前 `origin/main` 已具备以下真实能力：

1. 目标多 Alpha 父包的冻结代表腿预测可通过 Prediction Store manifest 和精确 `pred.pkl` 读取。
2. QE Qlib 日线 Bin、H5/`static_factors.parquet`、停牌文件和基准数据可按文件读取；训练不需要数据库。
3. `SharedAdvisoryFeatureBuilder` 已实现训练与正式推理的特征 schema parity。
4. `AdvisoryListTransitionEngine` 已是 Advisory 手工 review、Historical Range adapter 共用的中立生命周期引擎，表达 ENTER/HOLD/EXIT/WATCH/WAITING、rank exit、stop loss、fixed/trailing take profit、time stop、替换预算和缺失 rank 语义。
5. M1/M5A 仍使用固定 406 decision dates、单一 80 日 test 和 5 日超额收益标签；该结果只作为历史对照，不能继续承担新模型选择。
6. M3/M4 已产生多期限 outcome、holding range 和价格范围模型，但其训练标签不等于 Program 真实 review policy 下的 episode 净收益。
7. P0-A/P0-B 的每日自然日前向发布与动态 bundle 分发已有独立待合入源码 PR；本设计不得把其源码合入、DDL、后端重启、调度激活或首日发布误写成当前生产事实。

当前缺口是：训练目标仍与实际最长 20 个交易日的 review policy 不一致，模型选择仍受单次 holdout 和已消费 test 限制，也没有候选级 take/skip 与 Top5 组合 policy 的分层评价。

## 2. Scope / 交付目标

P0-C 只交付离线、文件驱动的标签和稳健评价能力：

1. 从一个精确冻结的 Advisory Program/package/binding、QE 文件版本、Prediction Store 代表腿和 shadow policy 构造可重复请求。
2. 每个 decision day 重建至少 Top40 的 runtime-equivalent selection rank，而不是沿用现有 Top20 candidate builder 截断结果。
3. 为当日 Top20 每个候选建立独立反事实 episode，按照冻结 Top5 shadow review policy 逐交易日推进，生成 policy-realized take/skip 标签。
4. 用同一日历和政策重放 Top5 shadow portfolio，表达 target count、持仓继承、替换预算和现金状态；不得以候选平均收益冒充组合收益。
5. 基于 episode 的实际信息区间生成 purged rolling/CPCV train-validation paths；PBO 或等价选择偏差只从全部 validation path 的 trial/family 结果计算。
6. 产出可供 P0-D WSL 训练直接消费的 rank/label/split Parquet/JSON 文件合同，不训练模型、不预拟合全区间 HMM、不选择阈值、不激活 bundle。

## 3. Non-goals / 明确禁止

- 不读取 PostgreSQL、Paper、模拟盘、QMT、Historical Range batch、历史 episode 或 R4/R5 snapshot 作为训练输入。
- 不回填、归档、修复或固化历史 Advisory 运行事实，不建设历史证据平台、通用数据平台、缓存平台或 ModelOps。
- 不修改 StrategyPackage、Selection Center、Paper v2、模拟盘、QMT、QE 或 P0-A 前向发布的业务逻辑和写入路径。
- 不改变生产 Top20 Program policy，不把 shadow Top5 结果写入生产 list、episode、metrics 或 Selection rank。
- 不使用分钟线；本阶段的 entry/exit basis 均为 `next_open_executable`，日线 open/high/low/close、涨跌停和停牌文件足以表达已批准标签。
- 不用未来 MFE/MAE、退出原因、episode path 或 target/future market 字段作为模型特征。
- 不复用已消费 80 日 test 选择 family、trial、阈值、特征、政策或窗口。
- 不增加审批、角色、人工 ACK、收益门槛或模型激活门禁。CPCV/PBO 是研究评价方法，不是业务门禁。
- 不在 Windows 训练模型；P0-D 训练仍限定 WSL Conda。P0-C 的纯文件构造器可在 Windows 单元测试，但正式产物必须由 WSL CLI 生成。

## 4. Contracts / 不可变身份与业务契约

新增 `FrozenAdvisoryPolicyDatasetRequestV1`，功能身份至少包含：

```text
schema_version = frozen_advisory_policy_dataset_request_v1
program_id
binding_version_id
package_id
manifest_sha256
package_asset_closure_hash
style_profile_id / style_profile_hash
selection_runtime_semantics_id / hash / payload
representative_seed_run_ids
representative_model_asset_sha256
prediction_artifacts (run/artifact/hash/rows/date range)
terminal_weights
qlib_daily_root
suspend_data_root
prediction_store_root
repository_root / repository_commit
decision_date_start / decision_date_end
data_cutoff
benchmark_instrument
baseline_policy / baseline_policy_sha256
shadow_policy / shadow_policy_sha256
price_semantics_version
cost_policy / cost_policy_sha256
split_policy / split_policy_sha256
output_root
resource_max_rss_bytes
```

`request_sha256` 由除 `created_at`、`output_root`、`request_id`、`request_sha256` 外的功能字段 canonical hash 得到，`request_id=advpolreq_<hash前24位>`。相同功能输入必须产生相同请求身份；输出位置和创建时间不能旋转业务身份。

### 4.1 Shadow policy

`model_shadow_review_policy` 从冻结生产 baseline policy 确定性派生：

- `target_count=5`
- `rank_enter_threshold=5`
- `rank_exit_threshold=40`
- `rank_exit_confirm_days`、`daily_replacement_budget`、`stop_loss_bps`、`take_profit_bps`、`trailing_stop_bps`、`time_stop_days`、`take_profit_mode` 与 baseline policy 相同
- `entry_price_basis=next_open_executable`
- `exit_price_basis=next_open_executable`

任何政策字段变化均旋转 policy hash、request、labels 和后续 model identity。不得只按名称复用旧标签。

### 4.2 Cost policy

本阶段必须使用显式、冻结且可审计的单边成本口径：

```text
buy_cost_bps
sell_cost_bps
minimum_commission_bps = 0
cash_return_bps_per_day = 0
benchmark_instrument = 000300.SH
```

成本值来自请求材料，不从运行环境或数据库隐式读取。候选 episode 的净收益为价格收益减买卖两边成本；基准 episode 使用相同 entry/exit 交易日 open-to-open 收益，不扣股票交易成本；净超额为两者之差。若当前批准请求未提供成本值，请求准备命令必须明确接收，禁止默认猜测。

## 5. File Inputs / 文件输入合同

### 5.1 Prediction Store

- 只解析请求中精确列出的代表腿 `run_id`。
- 每个 manifest 必须恰有一个 `pred.pkl`，artifact hash、size、row_count、date range 与请求一致。
- 每个交易日按父包终端权重和现有 runtime-equivalent normalization 组合腿分数。
- 候选构建使用 `raw_top_k >= 40`、`target_count=40`；训练候选集取当日 rank 1..20，rank 21..40 仅用于持仓退出判断。
- 每个活跃 episode 即便跌出 Top40，也要有显式缺席 rank：仅当当日 ranking observation 完整覆盖 Top40 时，缺席可表示为 synthetic rank 41；预测数据不完整时整日标记 `DATA_UNAVAILABLE`，不得把缺行当跌出。
- `decision_date_start..decision_date_end` 只定义产生训练候选的 decision days；为这些候选推进最长20日episode，rank context 必须继续用同一冻结代表腿重建至 `data_cutoff` 前一交易日。context-only日期不产生新候选标签，也不进入CPCV decision groups。

### 5.2 QE daily Bin / suspend Parquet

- Qlib calendar 是唯一交易日日历。
- Qlib 日线必须读取 `open/high/low/close/prev_close/up_limit_price/down_limit_price/limit_up/limit_down/factor`。
- `suspend_d` 文件必须覆盖目标 symbols 和请求日期范围。
- benchmark 必须具有同一 entry/exit 交易日的 open。
- Qlib日线和停牌文件 cutoff 必须覆盖 `decision_date_end` 后最多 `time_stop_days + rank_exit_confirm_days + 2` 个交易日；不覆盖则尾部 episode 为 censored，而不是补零。
- 涨跌停标记与价格来自已有 Bin；不调用 Tushare，不补抓网络数据。
- P0-C 不使用静态factor H5，因此不校验或复制全套factor文件；P0-D按实际特征需求在各CPCV path内读取并验证，不能让无关factor完整性阻断标签构建。

### 5.3 Feature boundary

P0-C 不发布可直接训练的 `candidate_features.parquet`。原因是 CPCV 的每条 train/validation path 都必须只在该 path 的 train 数据内重拟合 HMM、缺失值处理、分类词汇和连续目标裁剪；把全区间或单一旧 split 的 HMM states 预先固化会向早期 validation path 泄漏未来信息。P0-D 复用现有 `SharedAdvisoryFeatureBuilder` 的基础行情/静态特征公式，但为每条可计算 path 在 train 内重训 HMM并投影 validation。

P0-C 的 rank/label 与 P0-D path-local 特征通过以下键一对一连接：

```text
program_id
binding_version_id
package_id
manifest_sha256
decision_as_of_trade_date
target_trade_date
instrument
```

## 6. Architecture / 日期时钟与事件架构

```text
FrozenAdvisoryPolicyDatasetRequestV1
  -> exact Prediction Store leg artifacts
  -> Top40 runtime-equivalent rank reconstruction
  -> QE daily/suspend/benchmark file projection
  -> candidate counterfactual episode label adapter
       (all Top20 independently enter when executable)
       -> existing AdvisoryListTransitionEngine for later-day exits
  -> Top5 shadow portfolio simulator
       -> existing AdvisoryListTransitionEngine for capacity/replacement lifecycle
  -> purged rolling/CPCV paths
  -> PBO research receipt
  -> immutable policy dataset bundle
```

### 6.1 日期与价格语义

对于 decision day `D`：

1. `target_trade_date=T` 是 Qlib calendar 中 D 的下一交易日。
2. D 的预测分数只代表 D 收盘后可见的 selection；候选不得使用 T 或之后的特征。
3. 反事实入场只在 T 的开盘可执行：存在合法 open、非停牌，且开盘未处于阻止买入的涨停状态。
4. 每个后续交易日 R 的退出判断使用 R 开盘价作为 mark，并使用 R-1 收盘后可知的 selection rank。这样不会以 R 收盘后产生的 rank 决定 R 开盘交易。
5. stop loss、take profit、trailing stop、time stop 和 rank exit 的判断均基于同一个 R open mark，优先级完全委托 `AdvisoryListTransitionEngine`，不得复制第二套退出顺序。
6. A 股 T+1：有效入场日当天不允许退出；首个退出判断日是下一交易日。`defer_stop_before_effective_entry` 与生产 review 语义一致。
7. high/low 只用于数据完整性和后续诊断，不用于在日内声称先触发哪个止盈/止损；本阶段不得根据日线 high/low 伪造不可判定的盘中成交顺序。

### 6.2 可交易性

- 停牌：入场日停牌则 `NOT_ENTERED_SUSPENDED`；持有日停牌则 episode 冻结，不增加有价格的 holding day、不触发退出，等待下一可交易日。
- 开盘涨停：候选入场不可执行，`NOT_ENTERED_LIMIT_UP`；不以 close 代替。
- 开盘跌停：需要退出但不可卖出时写 `EXIT_DEFERRED_LIMIT_DOWN` 并保留 episode，下一可交易日继续。
- 缺价或非法价格：该日 `DATA_UNAVAILABLE`；候选或组合路径均不得跨过未知日继续伪造结果。
- `factor` 仅用于复权一致性检查，entry/exit 使用请求明确的同口径 open。

## 7. Candidate Episode Labels / 候选级标签

每个 D 的 Top20 symbol 独立建立一个反事实 episode，不受 Top5 组合容量限制。候选入场 adapter 在可执行 target open 上按 `AdvisoryTransitionEpisodeV1` 的同一字段合同直接建立 episode，保留真实 selection rank；它不调用 Top5 `rank_enter_threshold`，否则 rank 6..20 会被错误排除。入场后的每日 mark、退出优先级、rank confirm、time stop 和 deferred 状态必须调用 `AdvisoryListTransitionEngine`。只有 Top5 portfolio replay 才执行 `target_count=5/rank_enter_threshold=5/daily_replacement_budget`。

标签状态：

```text
MATURED
CENSORED_RIGHT_BOUNDARY
NOT_ENTERED_SUSPENDED
NOT_ENTERED_LIMIT_UP
NOT_ENTERED_MISSING_OPEN
DATA_UNAVAILABLE
```

成熟 label 至少包含：

```text
episode_label_id
request_id / request_sha256
program/package/binding/manifest identities
shadow_policy_sha256 / cost_policy_sha256
decision_as_of_trade_date / target_trade_date
instrument / selection_rank / selection_score
entry_trade_date / entry_price
exit_signal_date / effective_exit_date / exit_price
holding_trading_days
exit_reason
gross_return_bps / net_return_bps
benchmark_return_bps / net_excess_return_bps
take_label
confidence_target
label_status
label_information_start / label_information_end
```

标签定义：

- `take_label=1` 当 `net_excess_return_bps > 0`，否则为0。
- `confidence_target` 是经过固定 winsor 边界后的连续 `net_excess_return_bps`，winsor 边界必须只从相应 train path 拟合；原始 label 文件保存未裁剪值，不能全数据预处理。
- 非 `MATURED` 行的 `take_label/confidence_target` 为 null，不参与训练或指标分母。
- 未入场状态不是亏损标签，也不是 skip=1；其可交易性可作为独立覆盖指标和未来模型输入，但不得被伪造为收益观察。
- 每个 label 保存实际 `label_information_end`；purge 根据真实结束日判断，不只用固定偏移近似。

## 8. Shadow Portfolio Replay / Top5 组合重放

组合 evaluator 使用同一冻结 shadow policy 和同一 `AdvisoryListTransitionEngine`：

1. 每日输入是一个 ranking provider 产生的完整 Top40；当前 P0-C 至少提供 `selection_rank_top5`，并预留 P0-D 注入 meta-label/reranker/HMM/random ranking 的纯接口。
2. 组合最多5个 active episodes；已有持仓优先按 policy 推进，只有退出产生空位后才允许新入场。
3. `daily_replacement_budget` 同时约束 rank-drop exits 和后续补位，严格沿用引擎返回语义。
4. 不可入场的候选保持现金，不递补超出当日 ranking provider 明确输出的候选，除非 provider 本身定义了确定性的后备顺序。
5. 每日保存 active count、cash slots、entered/held/exited/waiting、turnover、gross/net/benchmark/excess return 和 drawdown。
6. 未成熟尾部组合 episode 按 mark-to-last-authoritative-open 单独报告 open exposure，不进入 completed-episode hit rate。
7. candidate evaluator 的 AUC/PR-AUC/Brier/coverage 与 portfolio evaluator 的累计净收益、最大回撤、turnover、hit rate 分开；禁止互相替代。

## 9. Purged Rolling / CPCV

### 9.1 Fold construction

- 输入只包括 `MATURED` label 的 decision groups。
- 按时间连续划分 `N` 个非空 group；默认 `N=8`，请求可显式覆盖，但不得按结果动态改变。
- 每条 CPCV path 选择 `K` 个 group 作为 validation；默认 `K=2`。
- train 是其余 group，但任何 train label 的 `[label_information_start, label_information_end]` 与 validation decision interval 相交时必须 purge。
- validation interval 前后额外 embargo `E` 个交易日；默认 `E=20`，且不得小于 shadow policy `time_stop_days`。
- fold/path 身份由请求、日期成员、purged members、embargo 和 policy hash canonical 派生。
- 路径中 train 或 validation 的成熟日期、正负类或每日 group 不足时状态为 `NOT_COMPUTABLE`，不得合并相邻路径或回看 test 调整。

### 9.2 Frozen historical test

旧 M1/M3/M4/M5A 的 80 日 test 不再是新管线的 test split，也不能进入 family/trial selection。若它与 P0-C 日期范围重叠，只能在最终报告的 `historical_reference` 区展示旧已发布指标；P0-C 不重新以该 test 选择任何内容。

### 9.3 PBO

PBO 输入是一张完整矩阵：每个候选 `family_id/trial_id` 在每个基础时间块上的同一政策净收益度量。方法身份固定为 `advisory_block_score_cscv_pbo_v1`：使用 CPCV 的 `N=8` 个按时间连续基础块作为 `S`，枚举 `C(8,4)=70` 个互补 in-sample/out-of-sample 块集。CPCV 仍生成 `C(8,2)=28` 条两块 validation path；每条 path 的逐日结果回归其两个基础块后聚合为 trial/block score，不能把28条重叠path直接当独立样本。实现必须：

- 保存全部 trial/path 结果，不只保存 winner。
- 对每个70组合划分，在4个块上按预声明主指标选择最佳 trial，并在互补4块上计算该 trial 的相对 rank/logit；主指标和 tie-break 必须在请求中冻结，不能看结果后选择。
- `PBO = OOS logit < 0 的比例`，并保存样本数、rank tie 规则和分布。
- trial 少于2、可配对路径不足或结果含缺失时返回 typed `NOT_COMPUTABLE` 和原因。
- PBO 只报告选择偏差，不自动否决模型、不形成激活审批。

P0-C 自身没有模型 trial 时仍生成 CPCV paths 和 `NOT_COMPUTABLE_NO_TRIAL_RESULTS` 的空 PBO receipt；P0-D 将同一 evaluator 用于真实 family/trial 矩阵。

## 10. Components / 代码边界

新增模块限定在 `backend/services/advisory_model_first`：

```text
policy_contracts.py          frozen request/policy/cost/split contracts
policy_rank_source.py        exact Top40 reconstruction and completeness
policy_episode_labels.py     candidate counterfactual episode labels
shadow_portfolio_policy.py   Top5 portfolio replay and metrics
policy_cpcv.py               purged rolling/CPCV paths and PBO
policy_dataset_bundle.py     immutable Parquet/JSON bundle publication/readback
policy_dataset_pipeline.py   bounded-memory orchestration
```

新增显式 CLI：

```text
scripts/advisory_policy_dataset_prepare_request.py
scripts/wsl/advisory_policy_dataset_build.py
```

现有模块最小改动：

- `candidate_group.py`：将固定两腿构造核心抽成可指定 `raw_top_k/target_count` 的现有参数路径，不改变 M1 的默认25/20。
- `qe_file_source.py`：只在已有 loader 缺少精确字段/分块读取时扩展，不建立新缓存或数据库 adapter。
- `AdvisoryListTransitionEngine` 不修改业务顺序；若离线 adapter 需要涨跌停 deferred 状态，在 adapter 外把不可执行 mark 表达为 WAITING，不扩展生产语义。

禁止 P0-C import `advisory_historical_range`、Paper、Simulation Runtime、Selection repository 或 PostgreSQL pool。

## 11. Bundle Contract / 产物

输出根：`<output_root>/policy_datasets/<policy_dataset_bundle_id>/`。临时目录完成后原子 rename；已存在相同 bundle 只允许逐文件 hash 一致的幂等 readback。

```text
manifest.json
request.json
baseline_policy.json
shadow_policy.json
cost_policy.json
candidate_rankings.parquet
candidate_episode_labels.parquet
candidate_label_coverage.json
shadow_selection_daily.parquet
shadow_selection_episodes.parquet
cpcv_paths.json
pbo_receipt.json
resource_report.json
source_schema_receipt.json
```

`policy_dataset_bundle_id` 由 request hash 和所有功能产物的内容 hash 派生；manifest 最后写。manifest 必须逐文件保存 sha256、size 和行数（适用时）。readback 必须验证全部文件，不能只信 manifest 字段。

不复制源 QE/Prediction 文件，不构建 SQLite，不生成历史数据库快照。

## 12. Resource and Failure Semantics

- 正式构建在 WSL `rdagent-gpu` Conda 运行，即使本阶段不训练 GPU 模型；保持与 P0-D 同一文件路径和依赖环境。
- 峰值 RSS 上限默认8GB；按日期块读取/处理 prediction 和 market rows，阶段结束释放 DataFrame，必要中间表以 Parquet 分片落盘后合并 metadata，不把全市场分钟或全历史矩阵常驻内存。
- 每阶段输出 wall time、peak RSS、row/date/symbol counts 和当前 stage。
- 任一 identity、schema、日期、rank coverage、market path 或 hash 不一致均抛 typed `AdvisoryModelFirstError` 并保留已完成非正式 run receipt；不得返回空成功、填0、跳日或改用数据库。
- 无成熟 labels、单类 path、PBO 不可计算是 typed research result，不是系统异常；它们不得被记为 PASS 或伪造数值。

## 13. API / UI / Database Contracts and Impact

- API：none。
- UI：none。
- DDL/DML：none。
- 后端重启：none。
- P0-A scheduler/runtime：none。
- Paper/Simulation/QMT/QE experiment writes：none。

P0-C 产物只由后续 P0-D WSL 训练读取；不会因源码合入自动执行或自动激活。

## 14. Verification Plan / 验证方案

### 14.1 Contract and identity

- request canonical identity 对创建时间/输出根不敏感，对 policy/cost/prediction/source commit 变化敏感。
- exact Prediction manifest/hash/roster、QE schema、calendar 和 data cutoff 读回。
- bundle 全文件 hash/size/row count readback；部分目录和同 id 不同内容 fail closed。

### 14.2 Episode semantics

- 固定人工路径覆盖 entry、hold、rank confirm exit、stop loss、fixed/trailing take profit、time stop、replacement budget。
- 同一事件路径与 `AdvisoryListTransitionEngine` 决策逐日一致。
- target open 入场、前一日 rank 决定下一 open 退出，无同日或未来 rank 泄漏。
- 停牌、涨停入场、跌停退出 deferred、缺价、有效 empty rank、Top40 缺席、尾部 censoring 全部有独立断言。
- 成本、benchmark 和 net excess 手算样本一致。

### 14.3 CPCV/PBO

- 任一 train label information interval 不与 validation/embargo 相交。
- 相同 dates/policy 产生相同 fold/path identity；乱序输入不改变结果。
- 每个 trial/path 保留；PBO 手算矩阵一致。
- 不足路径、单 trial、缺失结果明确 `NOT_COMPUTABLE`。
- 旧80日 test 不出现在 selection inputs 或阈值拟合路径。

### 14.4 Portfolio

- Top5 容量、替换预算、现金、持仓继承和退出后补位逐日一致。
- candidate metrics 与 portfolio metrics 分开。
- random 对照必须由请求固定 seed，不能每次变化；P0-C 不用随机结果选择 policy。

### 14.5 Real WSL smoke

- 使用现有目标多 Alpha 的真实 QE 文件和代表腿预测构建非空 bundle。
- 至少覆盖完整20日成熟窗口，Top40 每日完整。
- 读回 label 状态分布、exit reason、CPCV path 数、PBO状态、portfolio日数、峰值RSS和总耗时。
- 相同 request 精确重跑得到相同功能文件 hash；时间戳型 receipt 可不同，但不得进入 bundle identity。

## 15. Rollout / Rollback

### 15.1 Rollout

1. 合入 P0-C 源码不会自动执行任何任务。
2. 用户授权后在 WSL 生成正式 policy dataset bundle；该动作只写显式 repo-external `output_root`。
3. P0-D 详细设计绑定一个精确 P0-C bundle id/hash，训练真实 meta-label。
4. P0-A/B 合入和运行时启用独立进行；P0-C 不等待前向 labels 成熟，但后续模型质量以未来 challenger observation 为最终 OOS。

### 15.2 Rollback

- 源码回滚不触碰业务数据库或其它模块。
- 未激活的 policy dataset bundle 可保留为研究产物；不得自动删除或归档。
- 标签语义发现错误时注册 BUG、旋转 request/bundle 并重建，不原地改写已发布 bundle。

## 16. Risks and Mitigations

| 风险 | 处理 |
|---|---|
| Top20 预测不足以判断 rank40 | 构造器必须读取至少Top40；不足即整日不可用 |
| 日线无法判断盘中触发顺序 | 只按 next-open review 语义，不用 high/low 伪造成交 |
| 尾部没有完整20日路径 | 显式 right-censored，标签为空 |
| 不可交易被误记负收益 | NOT_ENTERED/WAITING 与 matured take/skip 分离 |
| 候选收益冒充组合收益 | 独立 shadow portfolio simulator |
| CPCV 被误当未来 OOS | 文档、receipt 明确 research validation；未来 forward 才是OOS |
| PBO 不可计算被伪造 | typed NOT_COMPUTABLE，不形成门禁 |
| P0-C 变成历史平台 | 文件直读、固定产物清单、零DB/零API/零UI |
| 与生产 review 顺序漂移 | 复用 AdvisoryListTransitionEngine，并用 parity tests 固定 |
| 内存再次失控 | 日期分块、Parquet中间文件、8GB监测，不读分钟线 |

## 17. Design Acceptance Index

| ID | requirement |
|---|---|
| F-501 | 冻结 request 精确绑定 package/prediction/QE/policy/cost/split/repository identities |
| F-502 | 每个交易日完整重建 Top40，Top20 为候选，21..40与缺席语义只用于持仓观察 |
| F-503 | Top20 每个候选建立独立反事实 episode，退出事件复用现有 transition engine |
| F-504 | target open 入场与前一 decision rank 决定下一 open review，无未来泄漏 |
| F-505 | 停牌、涨跌停、缺价、不可入场、退出延迟和右删失均为 typed 状态 |
| F-506 | 显式成本、benchmark open-to-open 和 net excess 可手算复核 |
| F-507 | Top5 shadow portfolio 独立执行容量、替换预算、持仓继承和现金状态 |
| F-508 | CPCV 按真实 label information interval purge 并覆盖 policy horizon embargo |
| F-509 | PBO 保存全部 trial/path 并在不可计算时返回 typed 状态，不形成审批门禁 |
| F-510 | policy dataset bundle 内容寻址、原子发布、全文件 readback 和精确重试 |
| F-511 | 正式构建仅在 WSL、峰值RSS不超过8GB、按日期块和文件中间产物处理；不固化全区间HMM训练特征 |
| F-512 | 零 DB/API/UI/runtime/shared-business write，不引用 Historical Range/Paper/Simulation |
| F-513 | 已消费80日test完全排除于新 family/trial/threshold 选择 |
| F-514 | 无简化版、静默错误、业务漂移、自动激活、角色审批或额外门禁 |

## 18. Design Acceptance Matrix / 设计验收矩阵

设计期 `ready` 只证明合同、实现路径和可执行验证已闭合，不代表源码或真实 WSL 构建已完成。实现后必须用实际证据更新矩阵。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | `backend/services/advisory_model_first/policy_contracts.py` | `backend/tests/advisory_model_first/test_policy_contracts.py` | pass | none |
| F-502 | `backend/services/advisory_model_first/policy_rank_source.py` | `backend/tests/advisory_model_first/test_policy_rank_source.py` | pass | none |
| F-503 | `backend/services/advisory_model_first/policy_episode_labels.py` | `backend/tests/advisory_model_first/test_policy_episode_labels.py` | pass | none |
| F-504 | `backend/services/advisory_model_first/policy_episode_labels.py` | `backend/tests/advisory_model_first/test_policy_episode_labels.py` | pass | none |
| F-505 | `backend/services/advisory_model_first/policy_episode_labels.py`, `backend/services/advisory_model_first/shadow_portfolio_policy.py` | `backend/tests/advisory_model_first/test_policy_episode_labels.py`, `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py` | pass | none |
| F-506 | `backend/services/advisory_model_first/policy_episode_labels.py` | `backend/tests/advisory_model_first/test_policy_episode_labels.py` | pass | none |
| F-507 | `backend/services/advisory_model_first/shadow_portfolio_policy.py` | `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py` | pass | none |
| F-508 | `backend/services/advisory_model_first/policy_cpcv.py` | `backend/tests/advisory_model_first/test_policy_cpcv.py` | pass | none |
| F-509 | `backend/services/advisory_model_first/policy_cpcv.py` | `backend/tests/advisory_model_first/test_policy_pbo.py` | pass | none |
| F-510 | `backend/services/advisory_model_first/policy_dataset_bundle.py` | `backend/tests/advisory_model_first/test_policy_dataset_bundle.py` | pass | none |
| F-511 | `backend/services/advisory_model_first/policy_dataset_pipeline.py`, `scripts/wsl/advisory_policy_dataset_build.py` | `backend/tests/advisory_model_first/test_policy_dataset_pipeline.py`; artifact: `policy_dataset_bundle/manifest.json` | pass | none |
| F-512 | `backend/services/advisory_model_first/policy_dataset_pipeline.py` | `backend/tests/advisory_model_first/test_policy_boundaries.py` | pass | none |
| F-513 | `backend/services/advisory_model_first/policy_cpcv.py` | `backend/tests/advisory_model_first/test_policy_cpcv.py` | pass | none |
| F-514 | §19 repeated design/source review | `python -m nox -s advisory_modeling_backend` | pass | none |

## 19. DESIGN-COMPLIANCE-001

合入前逐项证明：

1. **无简化版**：真实 Top40、真实 policy episode、真实 portfolio replay、真实 CPCV paths 和真实文件 bundle；不能用固定 fixture 或旧5日标签代替正式路径。
2. **无静默错误**：身份、schema、rank、行情、不可交易、删失和 PBO 不可计算均有 typed 状态或异常，不补0、不跳日、不改源。
3. **无业务语义偏移**：生产 Top20、Selection、Paper、模拟盘和既有 M1-M5C 不变；shadow policy 与候选反事实评价独立。
4. **无未经确认门禁审批**：没有角色、审批、收益门槛、人工 ACK 或自动激活；必要的数据完整性校验在正确输入下自动通过。

正式设计审核记录：

- Round 1：补齐 F2 章节和 F-ID；把候选级反事实入场与 Top5 组合容量拆开，避免 rank 6..20 被静默排除。
- Round 2：固定 path-score CSCV PBO 的块数、互补集合、主指标和 tie-break 身份；避免“PBO/等价”成为不可验证口号。
- Round 3：对照现有 Prediction Store、QE file source、fixed split 和 transition engine，确认不依赖数据库、旧80日test或第二套退出顺序。
- Round 4：删除P0-C全区间candidate feature产物；要求P0-D在每条CPCV path的train内重训HMM/预处理，避免早期validation读取未来拟合状态。
- Round 5：删除P0-C无直接依赖的全套factor H5 schema门槛；补强baseline→shadow policy逐字段派生一致性与Top40 rank语义版本。
- Round 6：拆分candidate decision range与future rank context range；后者延伸至data cutoff，避免所有尾部候选因缺后续Top40而被系统性删失。
- Source Round 1：修复WSL worktree gitdir转换、成熟label非法信息区间和组合退出空episode解引用。
- Source Round 2：删除无直接依赖的factor schema门槛并强化shadow policy逐字段继承。
- Source Round 3：修复一字跌停已知open未计入组合mark与持有日的收益偏差。
- Source Round 4：candidate range与rank context range分离，context-only日只推进已有持仓，不产生新标签或入场。
- Real WSL Round 1：真实标签/组合/CPCV计算完成后，bundle JSON writer拒绝`Timestamp`；修复为显式date/datetime/numpy规范化并禁止NaN或任意对象静默字符串化。
- Real WSL Round 2：最终SHA `f3b3f40c`生成bundle `81e2c9ba...69bd`；7,720候选、7,716成熟、3个涨停未入场、1个右删失、28/28 CPCV paths READY、峰值1.72GB、28.9秒；相同request重跑命中相同bundle及功能文件hash。
- Result audit：take正例4,199、负例3,517；持有期中位6日/最大20日；退出原因为rank drop 6,200、stop loss 898、time stop 572、trailing take profit 46；rank 1..5至16..20 take rate均约54%，证明selection rank组内区分力弱，支持下一阶段meta-label方向。

## 20. Implementation Plan / 实施方案

1. 完成本详细设计的 F2 validator 和反复正式审核。
2. 实现 request、Top40 rank、candidate episode、portfolio replay、CPCV/PBO、bundle 和 WSL CLI。
3. 每发现一个设计偏差先修源码与定向测试，再复审；禁止以缩小范围或跳过状态完成。
4. 完成 changed-file lint/compile、直接合同测试、`advisory_modeling_backend`、scope/ownership/classifier 和 DESIGN-COMPLIANCE-001。
5. 建立 P0-C PR；源码合入必须等待用户确认。
6. 合入后正式 WSL policy dataset 构建属于独立实验动作；产物生成后再开始 P0-D 详细设计和真实训练。

## 21. Production Gates / 生产影响与独立授权

本阶段没有生产门禁或审批。动作边界如下：

| action | design/source状态 | 独立授权 |
|---|---|---|
| P0-C 源码/PR合入 | pending user confirmation | 必须 |
| DEV/生产 DDL/DML | none | 不适用 |
| backend/frontend restart | none | 不适用 |
| scheduler/runtime activation | none | 不适用 |
| 正式 WSL policy dataset 构建 | pending source merge and explicit experiment execution | 长任务后续实验步骤，不等同合入 |
| P0-D bundle训练/写入 | out of P0-C scope | 后续阶段 |

## 22. Completion Definition

P0-C 只有同时满足以下条件才可报告完成：

- F2 validator 和本 Design Acceptance Index 全部有直接实现/测试证据。
- 变更模块及真实直接依赖测试通过。
- 一个真实 WSL request 生成非空 policy dataset bundle，hash readback 和精确重试通过。
- episode 事件 parity、无未来泄漏、Top40覆盖、删失、成本、组合 replay、CPCV/PBO receipt 均可读。
- 没有数据库、服务、运行时、模型激活或其它模块写入。
- 如真实数据暴露不可计算项，保持明确状态并报告，不用简化逻辑绕过。

正式实验产物：

```text
request_id = advpolreq_bbc87a5590af0519caa07a2f
request_sha256 = bbc87a5590af0519caa07a2fc3778657e2a7ace03f24bc15131bc5ae6df63bdf
policy_dataset_bundle_id = 81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd
bundle_root = F:/Dev/AIstock_model_artifacts/advisory_model_first/policy_datasets/81e2c9ba...69bd
pbo_status = NOT_COMPUTABLE_NO_TRIAL_RESULTS (P0-D前的正确状态)
```
