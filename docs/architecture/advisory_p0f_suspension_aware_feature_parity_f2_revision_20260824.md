# Advisory P0-F 停牌感知特征与同覆盖对照 F2 详细设计修订

> 日期：2026-08-24
> Tier：F2
> 状态：`DESIGN_READY_IMPLEMENTATION_NOT_STARTED`
> 被修订设计：`docs/architecture/advisory_p0f_policy_utility_ranker_f2_design_20260824.md`
> 失败实验源码：local-only commit `58e9455d0bf0ed5e59316bf049a8b8e42cf1f779`
> 失败实验 bundle：`62950813ae58f6946e4280211cb7c703e46ddd7522b0ffa383a3c1596bd50f97`

## 1. Background / 修订结论与适用优先级

本修订解决旧设计中两个不能同时满足的合同：

1. 旧 P0-D feature builder 对 required feature 缺失执行 `drop_candidate`；
2. P0-F 要求每个 decision date 对 exact Selection Top20 全部评分，一行不少、一行不多。

真实 Stage A 运行证明二者冲突：P0-C 共 `7720=386*20` 条候选，旧 builder 只产出 `7651` 行；`69` 行分布在 `58` 个 decision date、`10` 只股票。数据库回读证明这些股票全部上市超过一年，决策日也全部不是当前停牌。缺失来自过去滚动窗口内的已验证停牌日：最近 60 个市场交易会话只有 `50..59` 个有效 `high`，严格 `min_periods=60` 令 `drawdown_60` 等 required feature 变为 NaN。

因此：

- 上市满一年过滤保持不变；
- 历史已验证停牌是正常市场状态，不得阻断日期、删除候选或缩减 CPCV；
- 旧 P0-D/P0-E bundle 保持不可变，仅作为 lineage reference；
- 本修订对旧设计的 feature schema、reference、advancement、Stage B confidence model 和实施顺序条款具有优先级；未被明确替换的 label、Huber、CPCV、policy、cost、PIT、证据分类和生产授权条款继续有效。

## 2. Scope / 目标与范围

### 2.1 目标

1. 对 exact P0-C 的 `386*20` 候选生成完整、可解释、PIT-safe 的停牌感知特征。
2. 正常停牌缺失的 `blocked_date_count=0`、`dropped_candidate_count=0`。
3. 使用版本化 feature schema v2，禁止以同一个 v1 hash 改写数值语义。
4. 在相同 schema、候选覆盖、labels、CPCV paths、family 和 seed 下重训 P0-D/P0-E/P0-F 三个实验臂。
5. P0-F advancement 只与同覆盖 P0-D v2 做主配对；旧 P0-D/P0-E 只报告 lineage 差异。
6. Stage A 仍然只产出 repo-external、不可变、未激活的离线 bundle。

### 2.2 Non-goals / 非目标

- 不改变上市满 365 天、ST、退市风险或交易所股票池规则。
- 不修改 P0-C labels、8 blocks、28 READY CPCV paths、Selection Top20/Top40、父策略包排序或 shadow/cost policy。
- 不在训练时查询实时数据库；训练只读取 request 绑定的 immutable Qlib/H5/suspend artifacts。
- 不把任意数据缺失都填零；只有权威 suspend sidecar 证明的停牌行可以自动归一化。
- 不覆盖或修改现有 P0-C/P0-D/P0-E/P0-F 失败 artifacts。
- 不在 Stage A 修改 API、UI、DB、scheduler、descriptor、active binding、Paper、Simulation 或 QMT。
- 不因为本修订增加生产审批、角色门禁或人工研究审批。

## 3. Architecture / 数据状态架构

### 3.1 `SuspensionAwareBarPolicyV1`

每个市场交易会话、每只股票必须先分类，再计算特征：

| bar_state | 判定 | 自动处理 | 是否允许删除候选 |
|---|---|---|---|
| `TRADED` | 无停牌记录，OHLC 有限且存在可成交行情 | 使用真实行情 | 否 |
| `SUSPENDED_VERIFIED` | immutable `suspend_d` 在该日标记 `S`，且 `suspend_timing` 为空或为 provider 全日 sentinel `09:30-09:30`；即使 provider 同时保留陈旧 OHLC | 生成显式停牌归一化行 | 否 |
| `MISSING_UNEXPLAINED` | 必需行情缺失且无停牌依据 | typed dataset failure | 否 |
| `SOURCE_CONFLICT` | 有真实成交行情但 sidecar 同日报停牌，或身份/日期冲突 | typed dataset failure | 否 |

状态判定优先使用 immutable suspend sidecar，但必须先区分全日与日内停牌。具有非空、非 `09:30-09:30` timing 的 `S` 是日内停牌，继续使用真实日线行情并不得生成全日零量行。全日停牌日存在 provider 陈旧 OHLC 不构成冲突；只有已分类为全日停牌的 sidecar 行与 raw bar 同时声称正成交量/正成交额，或无全日停牌记录且不存在可因果延续的全日 `S` 状态却缺少必需 raw identity，才是冲突/未知缺失。旧 v1 loader 默认行为保持不变，只有 schema v2 显式请求 `full_day_only`。

Tushare `suspend_d` 可能只保留连续停牌区间的全日 `S` 边界，而不在每个无行情交易日重复一行。`SuspensionAwareBarPolicyV2` 因此按 instrument 顺序执行因果状态机：已可见的权威全日 `S` 开启停牌状态；其后的零量或缺行情 session 延续为 `SUSPENDED_VERIFIED`；首个正成交量/正成交额 raw bar 立即清除状态并按真实复牌行情处理。该状态机不得读取未来复牌日或复牌价格，不得由日内 `S` 开启，不得隐藏负成交量等源错误；没有任何较早全日 `S` 权威的缺行情仍为 `MISSING_UNEXPLAINED`。

`MISSING_UNEXPLAINED` 和 `SOURCE_CONFLICT` 必须在 candidate feature build 之前失败；不得通过删除一只股票让任务继续。它们是数据集错误，不是负面模型结果。

### 3.2 上市与当前停牌边界

- `list_date + 365 days <= decision_as_of_trade_date` 继续由现有 PIT universe 保证。
- 历史停牌不影响股票池资格。
- 当前停牌候选保留在 Selection/rank/持仓证据上下文，不因 feature missing 被删除。
- 当前停牌仍由既有执行合同产生 `entry_available=false`；不得伪造成可成交，也不得在特征层私自改变 Selection rank。
- 退出、等待价格、现金槽和 replacement budget 继续由既有 shadow policy 决定。

## 4. 停牌感知行情归一化

### 4.1 固定市场会话轴

归一化面板以 request 绑定的市场交易日历为唯一会话轴。周末和法定休市日不生成行；股票停牌日生成 `SUSPENDED_VERIFIED` 行。

对 `TRADED`：保留真实 OHLCV/amount。

对 `SUSPENDED_VERIFIED`：

```text
normalized_close  = last_executable_close_at_or_before_session
normalized_open   = normalized_close
normalized_high   = normalized_close
normalized_low    = normalized_close
normalized_volume = 0
normalized_amount = 0
bar_state          = SUSPENDED_VERIFIED
```

约束：

- 归一化必须在 request 绑定的 canonical adjusted-price domain 中完成；adjustment factor 只能使用该 session 当时可见的 as-of 值，禁止混用 raw close、未来 corporate-action factor 或另一个数据源价格。
- 对 `SUSPENDED_VERIFIED`，`last_executable_close` 必须来自当前会话之前最近一个真实可成交 session；禁止采用当前陈旧 provider bar、后向填充或复牌价。
- 停牌日收益、振幅和当日 true range 自然为 0；这不是伪造成交，而是固定市场会话估值。
- 复牌日全部使用真实行情；`open_gap`、true range 和 return 必须相对停牌前最后可执行收盘价计算，完整保留复牌跳空。
- 原始 Qlib/H5 文件不改写；归一化面板只存在于版本化 feature build/bundle evidence 中。

当前 decision session 为 `SUSPENDED_VERIFIED` 时，`decision_limit_up/down`、涨跌停距离和其他依赖可执行盘口的量不得伪造。schema v2 将这类列从无条件 required 改为 suspension-conditional optional，并增加 missing indicator；候选行继续保留和评分，执行层仍明确不可成交。

### 4.2 固定窗口语义

`ret_N`、`drawdown_N`、ATR、volume/amount ratio 继续表示最近 N 个市场交易会话，不改成跨度不固定的“最近 N 个有成交日”。窗口在归一化面板上计算：

- price path 在停牌会话保持前收盘价；
- volume/amount 在停牌会话为 0；
- 复牌成交量相对包含停牌零量的窗口可能放大，这是可解释的复牌流动性信号；
- `TRADED` 输入下分母为0、无任何历史可执行收盘或上市年龄身份不符均 typed fail closed。

上条仅对非停牌输入错误适用。若 ratio、盘口距离或类似派生量仅因完整窗口均为 `SUSPENDED_VERIFIED` 而数学上无定义，则保留 NaN 并设置对应 `__missing=1` 和停牌统计，不得阻断或删除。若绑定历史窗口开始时股票已经停牌或尚未上市，则该 instrument 的面板从窗口内首个真实 executable bar 开始，禁止用未来复牌价反填前导 session；这不删除锚点后的候选。若整个绑定窗口均无 executable close，或候选决策发生在首个锚点之前，则视为 source/identity 或 exact-coverage 异常并 fail closed。

### 4.3 明示停牌特征

新增以下模型特征，禁止让模型把合成的零波动误认为普通平稳行情：

```text
suspend_session_count_5
suspend_session_count_20
suspend_session_count_60
suspend_fraction_20
suspend_fraction_60
sessions_since_last_suspend
current_bar_synthetic
zero_liquidity_window_5
zero_liquidity_window_20
```

这些列全部进入 schema v2；所有 suspension-conditional optional 派生量具有显式 missing indicator。`current_bar_synthetic` 只在 decision date 本身为已验证停牌时为 1。不存在停牌记录时不得仅根据 OHLCV 缺失猜测停牌。

## 5. Contracts / Feature schema v2 与身份

新 schema：

```text
schema_version = advisory_feature_schema_v2_suspension_aware
bar_policy_schema_version = suspension_aware_bar_policy_v2
candidate_coverage_policy = PRESERVE_EXACT_SELECTION_TOP20_V1
verified_suspension_price_mode = LAST_EXECUTABLE_CLOSE_MARKET_SESSION_V1
verified_suspension_liquidity_mode = ZERO_MARKET_SESSION_V1
unexplained_missing_mode = FAIL_DATASET_NO_CANDIDATE_DROP_V1
```

schema hash 必须覆盖：

- v1 identity/model columns；
- 新停牌特征及其顺序；
- bar policy 完整 canonical payload；
- market calendar identity；
- suspend sidecar file hash/cutoff；
- required/optional/categorical/missing-indicator 分类。

禁止：

- 保留 `advisory_feature_schema_v1` hash 但改变 rolling/fill 数值；
- 使用 `drop_candidate`、`drop_date` 或 Selection-rank fallback 达成 exact Top20；
- 在没有较早权威全日 `S` 状态时，将 unexplained provider gap 合并为停牌；
- 训练时扫描 latest sidecar、latest Qlib root 或数据库现态。

## 6. 三实验臂同覆盖合同

旧 P0-D/P0-E 使用 v1 `drop_candidate`，不能继续作为唯一主比较。新 Stage A 在同一次 frozen request 中预注册三臂：

| arm_id | objective | weighting | role |
|---|---|---|---|
| `ARM_P0D_V2_BINARY_PARITY` | binary take/skip | none | coverage-parity primary baseline |
| `ARM_P0E_V2_WEIGHTED_BINARY_PARITY` | binary take/skip | fixed existing outcome weighting | diagnostic |
| `ARM_P0F_V2_HUBER_UTILITY` | Huber continuous policy net excess | none | challenger |

三臂必须共享：

- exact P0-C bundle/labels；
- exact `7720` candidate identities；
- schema v2/bar policy/calendar/suspend sidecar；
- exact 8 blocks、28 paths、purge/embargo；
- `CORE/CORE_HMM`、三个 seed 和原固定 LightGBM 参数；
- Selection/shadow/cost policy 和 evaluator。

每臂 `2 families * 3 seeds * 28 paths = 168 trial-path`，三臂总计 `504 trial-path`。不得根据任一臂结果增加 family、seed、feature、imputation、rank guard 或目标函数。

每个 path 只构建一次内容寻址的 train/validation feature matrix；三个 arm 必须回读相同 candidate identity、feature hash 和 HMM state hash。训练按 arm/family/seed 顺序执行并在 trial 后释放 booster/dataset 临时对象，禁止为缩短时间同时复制三套全量矩阵突破 8GB 上限。

P0-D v2、P0-E v2 和 P0-F v2 的监督 loss 都只读取各自合同允许的 `MATURED` labels；validation scoring 和 shared-policy evaluation 必须覆盖全部20行，包括非 MATURED、当前停牌和最终不可进入的上下文行。不得用 label status 预先过滤预测池。

旧 exact P0-D/P0-E bundle identity 仍进入 request 和 receipt，标记为：

```text
LEGACY_LINEAGE_REFERENCE_NOT_COVERAGE_PARITY
```

它们不参与 v2 advancement gate。

## 7. Frozen request v2

新 request 类型：`FrozenAdvisoryPolicyUtilityTrainingRequestV2`。

除旧 v1 identity 外，新增并冻结：

```text
feature_schema_version = advisory_feature_schema_v2_suspension_aware
feature_schema_hash
bar_policy
bar_policy_sha256
market_calendar_root/hash/cutoff
suspend_sidecar_root/hash/cutoff
candidate_coverage_policy = PRESERVE_EXACT_SELECTION_TOP20_V1
training_arms = (P0D_V2, P0E_V2, P0F_V2)
legacy_p0d_reference_identity
legacy_p0e_reference_identity
expected_candidate_row_count = 7720
expected_decision_date_count = 386
expected_candidates_per_date = 20
```

request 继续绑定 clean repository commit、P0-C、Program/binding/package、shadow/cost/split policy、Qlib/H5 roots和 `model_information_cutoff_trade_date=2026-03-10`。

## 8. Stage A v2 流程

```text
exact P0-C rankings/labels
  -> market calendar + Qlib raw daily + immutable suspend sidecar
  -> classify TRADED / SUSPENDED_VERIFIED / invalid
  -> build suspension-aware normalized panel
  -> assert 7720 rows and exact 20 rows/date
  -> build feature schema v2 matrix without candidate/date drop
  -> shared CPCV paths
  -> train P0-D v2 / P0-E v2 / P0-F v2 arms
  -> deterministic Top20 rank for each arm/path
  -> shared shadow portfolio evaluation
  -> PBO + candidate diagnostics + coverage-parity paired comparison
  -> immutable Stage A v2 bundle
  -> advancement PASS or model NEGATIVE_STOP
```

正常停牌归一化不得产生 `NEGATIVE_STOP_INCOMPLETE_CPCV`。只有 path/train/validation identity 真正缺失才使用 incomplete；数据源异常使用独立 dataset failure reason，不得伪装成模型负面结果。

## 9. Ranking、执行与 portfolio 语义

- 每个 decision date、每个 arm/path 都必须输出 exact 20 行和 `entry_priority_rank=1..20`。
- P0-D/P0-E 按 `take_probability DESC, selection_effective_rank ASC, instrument ASC`。
- P0-F 按 `predicted_policy_net_excess_return_bps DESC, selection_effective_rank ASC, instrument ASC`。
- `selection_exit_rank=selection_effective_rank` 保持不变。
- 当前停牌或下一目标日停牌由执行层设置不可进入/不可退出；不得通过删除预测行修改排名证据。
- `prediction_coverage_count=20` 不等于 `executable_entry_count=20`。当前停牌行继续占有其真实模型 rank；是否留下现金槽、等待价格或由其他可进入候选成交，完全由现有 transition engine、Top5 enter threshold 和 replacement budget 决定，v2 不得私自用 rank6+ 补位。
- target count、rank enter/exit、replacement budget、止盈止损、trailing 和 time stop 不变。

## 10. Advancement v2

P0-F v2 winner 仍只由 shared-policy `mean_daily_net_excess_return_bps` 选择。进入 Stage B 必须同时满足：

1. candidate minus `ARM_P0D_V2_BINARY_PARITY` 28-path mean primary metric `>0 bps`；
2. candidate 对 P0-D v2 path win rate `>0.50`，tie 不计 win；
3. candidate minus matched Selection Top5 mean primary metric `>0 bps`；
4. paired mean maximum drawdown difference `>=0`；
5. paired mean turnover difference `<=0`；
6. 三臂各自 exact 168 trial-path，path identity 完整唯一；
7. feature coverage receipt 为 `7720 rows / 386 dates / 20 each / 0 drop`；
8. `MISSING_UNEXPLAINED=0`、`SOURCE_CONFLICT=0`。

第 7、8 项是实验输入完整性，不是生产审批。输入不完整时状态为 `DATASET_INVALID_NOT_A_MODEL_RESULT`；输入完整但模型 gate 失败时才是 `NEGATIVE_STOP_NOT_ADVANCED`。

P0-E v2、legacy P0-D/P0-E、PBO、candidate loss 和历史 replay 都不是 advancement gate。

## 11. Stage B 修订

只有 v2 advancement PASS 才允许 Stage B。Stage B confidence model 必须使用同覆盖 `ARM_P0D_V2_BINARY_PARITY` winner，不再使用旧 v1 P0-D 作为新 role 的 confidence model；旧 P0-D role/descriptor/API bytes 仍保持不变。

新 bundle 自包含：

- utility model + utility schema v2；
- coverage-parity confidence model + confidence schema v2；
- bar policy/calendar/suspend identity；
- 两模型所需 HMM assets；
- legacy lineage reference identity；
- exact Top20 coverage receipt。

utility score 仍是 bps，不伪装概率；take/skip 概率只来自 P0-D v2 binary arm。

## 12. PIT 与未来数据防泄露

- 停牌分类只读取 `session <= decision_as_of_trade_date` 的 immutable sidecar 行。
- `last_executable_close` 只允许 forward carry；禁止 backward fill、复牌价回填停牌期或全样本插值。
- 每条 CPCV path 的 label transform、categorical vocabulary、early stopping、停牌统计和任何 fitted state 只读取 path train 或该 decision 当时可见历史。
- 市场日历、sidecar、Qlib/H5 cutoff 与 file hash 进入 request；文件漂移生成新 request。
- future-poison 在 cutoff 之后添加极端行情、停牌/复牌和 label，必须证明旧 request、旧 feature rows 和旧 validation predictions 字节不变。
- 训练不读取数据库；数据库只可用于独立 DEV 诊断，不能成为 artifact 的隐式输入。

## 13. Failure semantics

| reason code | condition |
|---|---|
| `ADVISORY_SUSPENSION_BAR_POLICY_INVALID` | bar policy/hash/固定模式漂移 |
| `ADVISORY_SUSPENSION_SOURCE_CONFLICT` | raw bar 与 suspend sidecar 冲突 |
| `ADVISORY_SUSPENSION_UNEXPLAINED_MISSING` | 非停牌必需行情缺失 |
| `ADVISORY_SUSPENSION_LAST_CLOSE_UNAVAILABLE` | instrument 在整个绑定窗口无 PIT executable close |
| `ADVISORY_FEATURE_V2_COVERAGE_INVALID` | 非 7720/386/20 或存在 candidate/date drop |
| `ADVISORY_POLICY_UTILITY_ARM_ROSTER_INVALID` | 三臂/family/seed/path 不完整或重复 |
| `ADVISORY_POLICY_UTILITY_REFERENCE_NOT_PARITY` | advancement 错用 legacy reference |
| `ADVISORY_POLICY_UTILITY_DATASET_INVALID` | 输入错误，非模型负面结果 |

所有 failure receipt 只保存非秘密身份、计数、hash 和有限样例，不保存凭据或数据库响应正文。

## 14. 允许的实现范围

Stage A v2 允许新增/修改：

```text
backend/services/advisory_model_first/suspension_aware_bar_policy.py
backend/services/advisory_model_first/feature_schema_v2.py
backend/services/advisory_model_first/policy_utility_contracts.py
backend/services/advisory_model_first/policy_utility_training.py
backend/services/advisory_model_first/policy_utility_pipeline.py
backend/services/advisory_model_first/policy_utility_bundle.py
backend/services/advisory_model_first/meta_label_features.py
backend/services/advisory_model_first/shared_feature_builder.py
scripts/advisory_policy_utility_prepare_request.py
scripts/wsl/advisory_policy_utility_train.py
backend/tests/advisory_model_first/test_suspension_aware_bar_policy.py
backend/tests/advisory_model_first/test_feature_schema_v2.py
backend/tests/advisory_model_first/test_policy_utility_contracts.py
backend/tests/advisory_model_first/test_policy_utility_training.py
backend/tests/advisory_model_first/test_policy_utility_pipeline.py
backend/tests/advisory_model_first/test_policy_utility_bundle.py
backend/tests/advisory_model_first/test_meta_label_features.py
backend/tests/advisory_model_first/test_shared_feature_builder.py
```

约束：v1 默认调用和旧 P0-D runtime bytes 必须通过回归测试；不得为了复用大幅重构无关 feature/runtime 模块。

## 15. Implementation Plan / 实施顺序

1. 实现纯函数 bar-state classification 与 suspension normalization。
2. 实现 schema v2/hash 和 exact coverage receipt。
3. 在 shared builder 增加显式 v2 policy 路径；v1 `drop_date/drop_candidate` 默认行为不变。
4. 将 P0-F request 升级到 v2，并冻结三臂与 sidecar/calendar identity；P0-C labels 不重建。
5. 在同一次按 path 内容寻址的 feature build 上顺序训练三臂，复用 exact CPCV paths 和 evaluator。
6. 发布新的 repo-external v2 bundle；失败实验 bundle 不覆盖。
7. WSL 完成 504 trial-path 和 exact retry。
8. advancement 失败则停止 Stage B；通过后另建 Stage B revision。

## 16. Verification Plan / 验证方案

### 16.1 纯函数/数据状态

- 60-session 窗口内含 1、10、59 个 verified suspension 时，required price/volume features 均可计算。
- 停牌日 normalized OHLC 等于前一 executable close，volume/amount 为0；复牌 gap 使用真实复牌价。
- provider 只给出一次全日 `S`、其后连续多个 session 无行情时，全部因果延续为停牌；首个正成交 raw bar 当日恢复 `TRADED`，且修改该未来复牌价不得改变更早停牌行。
- 窗口起点处已经停牌时，不生成无锚点合成价格；面板从首个真实 executable bar 开始，整个窗口无锚点则 typed fail。
- 将复牌后未来价格改为极端值，停牌期和更早 decision features 不变。
- 相同 NaN 无 suspend record 时必须 typed fail，不得归一化。
- raw traded bar 与 suspend record 同日冲突必须 fail。

### 16.2 覆盖与回归

- exact P0-C 得到 `7720` feature rows、`386` dates、每天 `20`、零重复。
- 已发现 69 行全部恢复，且 `bar_state`/suspend counts 可解释。
- 当前 69 行在 decision date 均非 suspended 的诊断保持为回归 fixture/evidence。
- v1 builder 默认结果和现有 P0-D bundle/runtime contract 不变。
- 当前停牌 fixture 必须证明 prediction/rank row 仍存在、`entry_available=false`，且没有 rank6+ 私自补位或 policy 参数漂移。

### 16.3 实验

- 三臂各 168、总计504 trial-path，无缺失/重复。
- 三臂 path、candidate identity、feature hash 完全一致。
- 同一 path 的三臂必须回读相同 feature/HMM bytes；任一 arm 自行重建或漂移矩阵即失败。
- P0-F gate 只读取 P0-D v2 parity metrics；若注入 legacy metrics 必须拒绝。
- shared portfolio metrics、PBO、candidate diagnostics、coverage receipt 和 resource receipt 完整。
- peak RSS `<8GB`；exact retry 不重训并返回同一 bundle。

## 17. Production Gates / 生产门禁

| gate | 本设计/Stage A |
|---|---|
| API/UI | none |
| DEV/production DDL/DML | none |
| backend/worker restart | none，用户持有 |
| production descriptor/activation | out of scope |
| dependency install | none |
| model artifact | 仅显式 repo-external Stage A v2 root |

## 18. Rollout / rollback

Rollout：

1. 本 F2 revision 独立审核、validator PASS、合入。
2. 从合入后的最新 `origin/main` 创建新的代码 worktree；不得继续在旧失败 commit 上直接训练。
3. 保留失败 bundle 和 request，不修改、不激活。
4. 代码/测试通过后以 clean commit 生成新 request，执行真实504 trial-path。
5. Stage A PASS 只允许进入 Stage B 开发，不等于运行时或生产激活。

Rollback：设计 PR 可由普通 revert 回退；无 DB/runtime/artifact mutation。任何实现错误生成新 commit/request/bundle，不覆盖旧 artifact。

## 19. Risks / 风险与处理

| risk | treatment |
|---|---|
| 停牌 carry 被误认为真实成交 | 显式 `bar_state/current_bar_synthetic`，只用于特征估值，不写回 raw data |
| 复牌跳空被抹平 | 复牌日使用真实 OHLC，相对最后 executable close 计算 gap/ATR |
| 停牌零量放大 volume ratio | 作为真实流动性变化保留，并同时提供 suspend fraction |
| provider gap 被误判停牌 | 只有 immutable sidecar 的较早全日 `S` 可开启因果延续；无该权威时仍为 dataset failure |
| v2 改值却复用 v1 hash | 新 schema/bar policy hash，v1 回归 bytes 不变 |
| 只修 P0-F 导致比较偏差 | P0-D/E/F 三臂同覆盖重训，legacy 仅 lineage |
| 504 trials 增加耗时 | feature matrix/HMM 只构建一次；三臂共享只读输入，仍受8GB约束 |
| 输入错误被称为模型负面 | dataset invalid 与 negative model stop 分离 |
| 代码框架成功冒充模型成功 | design/source/training/advancement/Stage B/runtime 分开报告 |

## 20. Design Acceptance Index

| ID | requirement |
|---|---|
| F-901 | 真实失败证据正确归因为历史 verified suspension，不是新股或上市过滤失效 |
| F-902 | bar state 四态分类完整，正常停牌自动处理，未知缺失不静默填充 |
| F-903 | 停牌归一化 PIT-safe、仅 forward carry、复牌跳空完整 |
| F-904 | 固定市场会话窗口与停牌显式特征语义冻结 |
| F-905 | schema v2/bar policy/calendar/sidecar identity 内容寻址，v1 不漂移 |
| F-906 | exact 7720/386/20、零 candidate/date drop |
| F-907 | 当前停牌保留证据但不可成交，历史停牌不改变股票池资格 |
| F-908 | P0-D/E/F 三臂同覆盖、同 path/family/seed，legacy 仅 lineage |
| F-909 | request v2 精确冻结三臂和全部数据身份 |
| F-910 | P0-F advancement 只使用 P0-D v2 parity baseline |
| F-911 | dataset invalid 与 negative model result 严格分离 |
| F-912 | future-poison、source conflict、停牌/复牌直接测试完整 |
| F-913 | 504 trial-path、PBO、bundle、8GB、exact retry 完整 |
| F-914 | Stage B confidence 使用 P0-D v2，旧 role bytes 不变 |
| F-915 | 零 DB/API/UI/restart/activation，生产授权边界不变 |
| F-916 | 三轮设计审核、F2 validator、scope/diff 检查通过 |

## 21. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-901 | 本设计 §§1,3 | artifact: `62950813ae58f6946e4280211cb7c703e46ddd7522b0ffa383a3c1596bd50f97` | design_ready | none |
| F-902 | `suspension_aware_bar_policy.py` | `backend/tests/advisory_model_first/test_suspension_aware_bar_policy.py` | design_ready | none |
| F-903 | normalization pure functions | `backend/tests/advisory_model_first/test_suspension_aware_bar_policy.py` | design_ready | none |
| F-904 | `feature_schema_v2.py` | `backend/tests/advisory_model_first/test_feature_schema_v2.py` | design_ready | none |
| F-905 | schema/request/bundle contracts | `backend/tests/advisory_model_first/test_feature_schema_v2.py`; `backend/tests/advisory_model_first/test_policy_utility_bundle.py` | design_ready | none |
| F-906 | shared builder v2 coverage | `backend/tests/advisory_model_first/test_shared_feature_builder.py`; artifact: future `feature_coverage_receipt.json` | design_ready | none |
| F-907 | feature + shadow policy boundary | `backend/tests/advisory_model_first/test_suspension_aware_bar_policy.py`; `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py` | design_ready | none |
| F-908 | three-arm pipeline | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-909 | request v2 | `backend/tests/advisory_model_first/test_policy_utility_contracts.py` | design_ready | none |
| F-910 | parity paired comparison | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-911 | typed failure receipts | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-912 | PIT poison/source conflict | `backend/tests/advisory_model_first/test_suspension_aware_bar_policy.py`; `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-913 | WSL pipeline/bundle | `backend/tests/advisory_model_first/test_policy_utility_bundle.py`; artifact: future `resource_report.json` | design_ready | none |
| F-914 | conditional Stage B dual model | `backend/tests/advisory_model_first/test_policy_utility_runtime_inference.py`; `backend/tests/advisory_model_first/test_dynamic_model_binding.py` | design_ready | none |
| F-915 | boundary assertion | `backend/tests/advisory_model_first/test_meta_label_boundaries.py` | design_ready | none |
| F-916 | complete revision | validation-receipt: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0f_suspension_aware_feature_parity_f2_revision_20260824.md --tier F2` | design_ready | none |

`design_ready` 只表示设计条款、计划实现位置和验证 oracle 已冻结，不表示源码、模型、Stage B、运行时或生产已完成。

## 22. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：真实 Stage A 必须恢复7720行、完成三臂504 trial-path；fixture、少路径或仅恢复69行不能冒充完成。
2. **禁止静默错误**：verified suspension 显式归一化；unknown missing/source conflict typed fail；不允许 drop/fallback/默认排序。
3. **禁止改变业务逻辑**：上市/ST/Selection/policy/cost/exit 不变；v2 只修复正常停牌的特征观测并保证实验覆盖一致。
4. **禁止私增门禁审批**：coverage和dataset checks是输入正确性，不是人工审批；生产激活仍由用户单独决定。

## 23. Review record

- Round 1（PIT/数值语义）：发现初稿会把 sidecar 报停牌但 provider 保留陈旧 OHLC 误判为 source conflict，也未冻结 adjusted-price domain；连续停牌的 ratio `0/0` 和当前盘口距离仍可能再次触发 required-feature 删除。已改为 sidecar 优先、只有正成交与停牌冲突才失败，冻结 as-of adjusted domain，并将仅因 verified suspension 无定义的派生量改为显式 optional+missing indicator；复审通过。
- Round 2（业务/实验可比性）：发现“预测20行”可能被误写成“20行都可成交”，也可能用 rank6+ 私自补位；三臂若各自重建 feature/HMM 仍存在输入漂移。已冻结 prediction coverage 与 execution availability 分离、当前停牌保留真实 rank 且 transition policy 不变，并要求三臂回读同一 path feature/HMM bytes、顺序训练和 MATURED-loss/all-row-scoring 分离；复审通过。
- Round 3（合规/生产边界）：逐项复核 DESIGN-COMPLIANCE-001，确认没有 drop/fallback、没有同 hash 改值、没有把 dataset failure 称为模型负面、没有新增 DB/API/UI/restart/activation 或人工审批。F2 validator `16/16`、0 warning，quality guardrail 0 finding，`git diff --check` 通过；复审通过。

## 24. 设计完成定义

- F-901..F-916 每项都有实现位置、直接测试或 artifact oracle，且无未批准 gap。
- 三轮审核记录改为已完成并列出实际修订。
- F2 validator、quality guardrail 和 `git diff --check` 通过。
- 设计合入后才从最新主线开始代码修复；旧失败源码 commit 不直接合入。
