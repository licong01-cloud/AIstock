# AIstock 荐股模型优先垂直切片 F2 详细设计 v1.3

> 日期：2026-08-08
> Feature tier：`F2`
> 当前状态：`M0_M1_TRAINED_EXPERIMENTAL_SHADOW`
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v2.5
> 首个目标：原生多 Alpha 父包 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` 的 SHORT_REBOUND Top20→Top5 真实模型
> 训练边界：只在 WSL Conda 环境读取已有 QE H5/Parquet/Qlib Bin 和 Prediction Store PKL
> 推理边界：只在 Advisory 消费层读取数据库当前/实时输入，不修改 Selection、Paper、模拟盘或 QE
> 训练结果：request `advmreq_ac5959aa8dc14a25e3b8c139`；bundle `9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629`；WSL 128.921 秒；峰值 RSS 2,262,388,736 bytes；shadow binding 未激活
> 质量结论：80 日 test Top5 完整，但 5 日平均超额收益 `-0.0002833`，低于原始排名 Top5 `0.0085591` 和固定 seed 随机 Top5 `0.0055652`；该结果只能作为未校准实验影子模型，不得描述为排序优化成功

## 0. 文档定位与权威顺序

本文档落实父级蓝图 M0、M1 和 M2 的第一个可运行垂直切片。目标不是再建设训练数据平台，而是使用已经存在的 QE 文件，在 WSL 完成真实模型训练，再把真实模型结果接入 Advisory 页面。

发生冲突时按以下顺序处理：

1. 用户当前明确要求。
2. 父级蓝图 v2.5 的模型优先顺序、数据边界和模块隔离边界。
3. 本文档冻结的目标资产、候选、特征、标签、时间切分和 API 合同。
4. 当前源码中与以上内容不冲突的既有实现。

旧 `advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` 仅可作为公式和领域背景参考。其 Historical Range、Phase 1R、SEALED snapshot、CAS、source revision、capture/build bridge 和多年矩阵前置路径不得进入本切片实现。

## 1. Background / 已核实事实基线

### 1.1 目标父包与 Advisory Program

| 项目 | 冻结值 |
|---|---|
| package_id | `pkg_ma_8ec5e389fa2c5e484a1ac7e9` |
| manifest_sha256 | `f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016` |
| asset closure source | 读取该 manifest 现有成功 Selection artifact v2 的 `asset_closure_hash`；只复制身份，不重新执行 package admission |
| alpha_mode | `multi_alpha` |
| source run | `macb_7738e811293948eb_20240702_20260310_20260625T184334308696Z` |
| roster_hash | `7738e811293948eb` |
| combination | `ic_weighted`，按交易日 universe 归一化，weighted sum |
| historical combine reference | 逐日 walk-forward 权重的 `combined_prediction.pkl`；仅作已回测组合与数据读取诊断，不作为当前 runtime 候选权威 |
| current runtime weight mode | `frozen_backtest_terminal_weights`；LSTM=`0.6966591521`，FUNDGROWTH=`0.3033408479` |
| current normalization | `zscore`；逐腿逐日全 universe、population std `ddof=0` |
| current runtime seed semantics | 每腿只运行 `seed_run_ids[0]` 对应的代表模型；不得把代表结果复制出的伪 seed 离散度作为模型特征 |
| representative seeds | LSTM=`qear_run_fc5d506390b8f70651a790e6`；FUNDGROWTH=`qe_20260622_035058_ec76_L5` |
| representative model SHA | LSTM=`9c65fe85fa1e3e31a544c2f59608f6c295de9b7943c4b04aadef3fc34aac87fc`；FUNDGROWTH=`5f255fc454f02ace754c7c6bfbd8362b37696f18eb1729a488f942ebe0396620` |
| parent raw candidate depth | 父包 score artifact 先按当前 runtime 语义截取 `25`；模型 group 再取 Program `target_count=20` 的前 20 |
| current Program | `advp_3126dd77f9774d94850f37ad012f640f` |
| active binding | `advb_f860140caa314665ad60ac089ed84b3f` |
| binding effective_from | `2026-07-20` |
| Program target_count | `20`，首个模型不得自动修改为 5 |
| runtime HMM/risk/tradability | HMM/risk disabled；industry blacklist 为空；suspend capability enabled，但带 previous-day cutoff 生成下一交易日目标时现有 Advisory 将 target-day suspend filter 设为 false |
| Advisory decision clock | `decision_as_of_trade_date < target_trade_date`；目标日 Selection run 使用前一交易日固定 cutoff，模型特征不得读取目标日数据 |
| style profile | `short_rebound_pkg_ma_8ec5e389_v1`；`effective_package_oos_cutoff=max(package created 2026-07-02, current binding effective 2026-07-20)=2026-07-20`，此前 test 只能标记 retrospective holdout |
| selection runtime semantics | id=`advisory_multi_alpha_representative_terminal_top25_to20_v1`；`strategy_package.runtime_variant.canonical_json_sha256` hash=`83fc0475964df75a9a23db597567af5bf31543f6980170f9d924c650ea3eb692` |

目标包已进入系统并稳定产生选股结果。本切片不得调用 package health、asset eligibility、runtime admission 或其它二次准入逻辑来决定是否允许训练或推理。实现只读取上表已经显式冻结的包、manifest、Program 和 binding 身份；不重新验证策略包业务可用性。

上述 runtime semantics hash 使用项目 canonical JSON 对以下 payload 计算；Batch 1 只比较当前配置是否仍等于该已批准语义，不调用准入流程。配置变化时生成新 training request/semantics 版本，不修改或阻断现有规则荐股：

```json
{"decision_clock_version":"advisory_previous_close_target_next_trade_v1","hmm_enabled":false,"industry_blacklist":[],"normalization_method":"zscore","provider_version":"multi_alpha_live_selection_provider_v3","raw_top_k":25,"representative_seed_run_ids":{"a1_plus3_LSTM_h20":"qear_run_fc5d506390b8f70651a790e6","new_FUNDGROWTH_h20":"qe_20260622_035058_ec76_L5"},"risk_policy_enabled":false,"schema_version":"advisory_selection_runtime_semantics_v1","target_count":20,"target_day_suspend_filter":false,"terminal_weights":{"a1_plus3_LSTM_h20":0.6966591521,"new_FUNDGROWTH_h20":0.3033408479},"weight_policy_mode":"frozen_backtest_terminal_weights"}
```

### 1.2 精确多 Alpha roster

父包只有以下两条合法组件腿，禁止跨实验拼腿：

| leg_id | model | seed 数 | 父包权重 |
|---|---|---:|---:|
| `a1_plus3_LSTM_h20` | `__seed_LSTM_10D_hs64_d02__` | 33 | `0.6966591521` |
| `new_FUNDGROWTH_h20` | `__seed_LGBModel_conservative_v1__` | 5 | `0.3033408479` |

LSTM 腿 seed run：

```text
qear_run_fc5d506390b8f70651a790e6
qear_run_9a1defd5a1e2257a7255b78d
qear_run_a897779cf0a3c30e41a37efc
qear_run_2ab298df84dcf5a024dc6bd5
qear_run_21afc18b61dddd3a53f2fdac
qear_run_1099a628e0322a96d46faf93
qear_run_4ebb5ff58e47f5065bb82829
qear_run_a8ae1bdd6146ea632d3dcae7
qear_run_eaf48dfbe26a95bf58a4bb3b
qear_run_9487b5f53ff0913f5f09bf47
qear_run_6687739da093e3284a26e306
qear_run_c478082a3afbb4a1d98b8865
qear_run_b7323fab9f5255541a025982
qear_run_59126620bc7dcf0b175c2071
qear_run_f485b00a928d3b70b7360f19
qear_run_eb749aa2cd9c5e221148830d
qear_run_a34f16978092d3ecedf05b2b
qear_run_6b24a283dc4ff3bd6e9d68d1
qear_run_433114f81a72801452c926a0
qear_run_eda52a9488df4aa8553e634b
qear_run_e573090215b24806664ebde8
qear_run_b524652839421fb0d80d72a4
qear_run_25c421dd28515128f9d89486
qear_run_dcf9b2f0bca2979fb3f92acf
qear_run_150bfa41e8a6a2d1664b3b07
qear_run_a5e4e0c0e5caa938e93df68d
qear_run_bdfd65618510eeb5d940f205
qear_run_7d96269f3e2fba256ab904a3
qear_run_16bbe11ea1794c462ccab2b3
qear_run_adfecf69242b697021d6d56d
qear_run_3ed05fccaa8a6cd0062e0d7a
qear_run_e262916978fa9f7844422584
qear_run_880feffe3961738b775f4573
```

基本面成长腿 seed run：

```text
qe_20260622_035058_ec76_L5
qe_20260622_035058_ec76_L6
qe_20260622_171346_0e41_L1
qe_20260622_171346_0e41_L2
qe_20260622_171346_0e41_L3
```

38 个 run 的 Prediction Store manifest 均已只读核实存在且包含带 SHA256 的 `pred.pkl`。实现必须通过 `PredictionArtifactStore`/`ModelStoreService` 精确读取这些 manifest，不扫描 QE workspace，不使用 latest run。代表 seed 缺失会阻断 runtime-equivalent candidate；非代表 seed 缺失只令 full-ensemble diagnostic 显式 unavailable，不得以剩余 seed 的缩减 ensemble 冒充完整诊断。

### 1.3 合成预测、逐日权重与当前 runtime 语义

历史合成参考：

```text
F:\Dev\AIstock\rdagent_assets\multi_alpha_combine_backtests\
macb_7738e811293948eb_20240702_20260310_20260625T184334308696Z\
combined_ic_weighted\combined_prediction.pkl
```

| 属性 | 已核实值 |
|---|---|
| sha256 | `e0c571f65006ba381526389f0d2a4bb0efda00e2b2b6ef42f420ca8bd9fc1463` |
| 类型 | pandas DataFrame，MultiIndex `datetime,instrument` |
| 列 | `score` |
| 行数 | `1,802,507` |
| 日期数 | `406` |
| 日期范围 | `2024-07-04..2026-03-10` |
| per-window weights | scheme result `32`，406 个 apply date，同日期范围 |

`combined_prediction.pkl` 是父包已回测 walk-forward 组合的权威参考，但不是当前 Advisory runtime 的候选权威。当前父包正式运行使用每腿代表模型和冻结终端权重；训练候选必须复现这一当前 runtime 语义，不能把历史逐日权重组合冒充为当前 Selection 语义。

38 个 seed prediction 与逐日权重只用于以下两个诊断：

1. 重算历史 walk-forward combined reference；最大绝对误差超过 `1e-8` 或排序不一致时，以 `ADVISORY_MODEL_REFERENCE_COMBINATION_MISMATCH` 标记该诊断失败。
2. 报告代表 seed 与完整历史 ensemble 的 score/rank 差异，量化当前 runtime 与已回测组合之间的分布偏移。

诊断失败不得被吞掉或标成通过，但只要两个代表 seed、当前 runtime identity 和训练基础数据完整，就不阻断当前 runtime 等价模型训练。诊断字段不得进入首模 FeatureSchema，也不得把代表模型结果复制 38 次后计算伪造的零方差。

### 1.4 已有基础数据

| 数据 | 物理位置 | 覆盖与用途 |
|---|---|---|
| Qlib 日线 Bin | WSL `/home/lc999/data/qlib_bin` | `2018-08-01..2026-06-30`；OHLCV、factor、amount、`prev_close`、涨跌停价和真实 limit flags |
| H5/Parquet | WSL `/home/lc999/data/factor_data_versions/qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2` | `2018-08-01..2026-06-30`；量价、估值、资金、基本面、融资融券、筹码和行业 |
| suspend sidecar | WSL `/home/lc999/data/suspend_d_daily_candidate_20180801_20260630` | 历史停牌日，只读 |
| Prediction Store | Windows `F:\Dev\AIstock\rdagent_assets\prediction_store`，WSL `/mnt/f/Dev/AIstock/rdagent_assets/prediction_store` | 38 个精确 seed `pred.pkl` |
| combined prediction | 上述 combine run 路径 | 父包权威历史合成 score |
| Qlib 分钟 Bin | WSL `/home/lc999/data/qlib_minute_bin` | 本切片禁止读取 |

H5 文件均使用 `/data` key 和 `(datetime,instrument)` MultiIndex：

- `daily_pv.h5`：`open,close,high,low,volume,factor,amount`。
- `daily_basic.h5`：16 个 `db_*` 估值、换手和市值字段。
- `moneyflow.h5`：18 个 `mf_*` 大中小单及净流字段，单位合同为 share/CNY。
- `bak_basic.h5`：15 个收入、利润、资产和股东字段。
- `margin_detail.h5`：8 个融资融券字段。
- `cyq_perf.h5`：9 个筹码成本和获利盘字段。
- `sector_data.h5`：23 个申万 L2 行情、估值、资金字段及 `l2_code_id`。
- `static_factors.parquet`：123 列，包含上述字段及已计算的资金、价值、规模、流动性和 `PriceStrength_10D`。

## 2. Scope / 本切片交付

1. 实现精确父包 roster、两个 runtime 代表 seed 和完整历史诊断预测文件读取器。
2. 实现与正式推理共用的 `SharedAdvisoryFeatureBuilderV1`。
3. 用当前文件数据重新拟合行业 HMM 并生成因果状态概率。
4. 在 WSL 训练真实 LightGBM LambdaRank Top20→Top5 模型。
5. 输出可加载的最小 `AdvisoryModelBundleV1` 和非空 test 预测。
6. 实现数据库当前/实时 FeatureSource 和模型影子推理服务。
7. 增加 Advisory API 和页面 `EXPERIMENTAL_SHADOW` readback。
8. 保持多个 Advisory Program 独立；本切片先验证目标多 Alpha Program，合同同时支持单 Alpha Program。

## 3. Non-goals / 明确禁止

- 不读取生产数据库构建历史训练数据。
- 不执行 Historical Range、Phase 1R、capture、label bridge、SEALED、CAS、source revision、lease、fencing 或 recovery。
- 不处理、修复、归档或清理旧 batch、old root、orphan build 和历史 operation。
- 不修改策略包、Selection、Paper、模拟盘、QE 或 Qlib 业务逻辑。
- 不重新准入、审批、复核或健康检查策略包。
- 不新增用户、角色、审批、人工确认、champion/canary、ModelOps 或自动发布门禁。
- 不训练或读取分钟模型；不加载 33GB 分钟 Bin。
- 不使用旧 HMM 模型、旧状态、旧系数或旧预测作为新模型输入。
- 不把回测组合收益、持仓、成交、净值或人工选择结果作为监督标签。
- 不用规则、随机分数、mock、静态 JSON 或旧模型输出冒充真实模型。

## 4. Architecture / 目标架构

### 4.1 离线训练

```text
FrozenAdvisoryTrainingRequestV1
  -> ExactParentPredictionSource
       -> 2 representative Prediction Store pred.pkl for training candidates
       -> frozen terminal weights and current normalization semantics
       -> 36 remaining seed pred + scheme 32 daily weights + combined prediction for diagnostics
  -> QEFileMarketSource
       -> Qlib daily Bin
       -> H5/Parquet factor files
       -> suspend sidecar
  -> OfflineSelectionEffectiveTop20Builder
  -> FreshFileSectorHMMTrainer
  -> SharedAdvisoryFeatureBuilderV1
  -> time split + 5d labels
  -> temporary projected Parquet shards
  -> WSL LightGBM LambdaRank trainer
  -> test predictions + baseline report
  -> AdvisoryModelBundleV1
```

Windows 只生成显式 request、触发 WSL、读取日志与结果。读取、特征构建、HMM 拟合、LightGBM 训练和评估全部在 WSL Conda 环境执行。

### 4.2 正式影子推理

```text
Advisory Program + active binding + decision clock
  -> existing Selection authoritative selection_effective candidates
  -> first target_count=20 rows only
  -> DatabaseRealtimeFeatureSource (batch query)
  -> fresh HMM model + DB observations through decision date
  -> SharedAdvisoryFeatureBuilderV1
  -> loaded AdvisoryModelBundleV1
  -> model score/rank + Top5
  -> Advisory API
  -> Paper v2 Advisory page / model shadow panel
```

模型服务只消费 Selection 已经生成的候选，不重新运行策略包推理，也不写回 Selection artifact。模型失败只让影子结果进入 `MODEL_UNAVAILABLE`，现有规则荐股继续返回原结果。模型 group 必须同时绑定 `decision_as_of_trade_date` 和 `target_trade_date`，数据库特征 cutoff 固定为前者。

## 5. Contracts / Candidate Top20 身份

### 5.1 离线候选

历史候选阶段冻结为：

```text
OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2
```

每个历史样本同时定义：

```text
decision_as_of_trade_date = d
target_trade_date = next trading day after d
feature_cutoff = d close
entry_date = target_trade_date
```

逐决策日确定性步骤：

1. 读取两腿代表 seed 在 `d` 的全量 score；不得把完整 38-seed ensemble 或 walk-forward combined score替代当前 runtime 代表模型语义。
2. 按父包当前 `zscore` formula 生成每腿字段：`mean=population mean`、`std=population std(ddof=0)`、`normalized=(raw-mean)/std`；std 非有限或不大于 0 时整腿 normalized 显式为 0 并记录 constant-leg diagnostic。`leg_rank` 按 `normalized DESC,symbol ASC`，再按 symbol 取两腿共同 universe。
3. 使用冻结终端权重 LSTM=`0.6966591521`、FUNDGROWTH=`0.3033408479` 计算 combined score。
4. 按 `combined_score DESC, symbol ASC` 形成 raw rank，并像父包 score artifact 一样先截取 raw Top25。
5. 当前 HMM/risk disabled、industry blacklist 为空，不做二次乘权或风险排除。
6. Advisory 为下一交易日生成目标列表时并不知道目标日停牌状态，现有业务会关闭 target-day suspend filter；离线重建同样不得用 `target_trade_date` 的停牌或涨跌停信息筛选候选。`d` 日停牌/涨跌停只作为已知特征，不改变候选。
7. 取 raw Top25 的前 `Program.target_count=20`，保持父包原 rank 作为 `selection_source_rank`，再生成模型 group 内连续的 `selection_effective_rank=1..N`，其中 `N<=20`；不得从另一个策略包或其它日期补位。

训练候选不得从回测持仓或 Top25/Top50 portfolio result 提取。`filtered_pool_20260630` 和 PIT instrument ranges 只负责复现父包代表模型的合法输入 universe，不生成新的研究准入。suspend sidecar 只用于特征和标签可成交性，不参与下一交易日候选筛选。

设计阶段对 walk-forward combined reference 的只读 smoke 得到 406/406 个日期、每日深度 20，raw Top25 内停牌数均为 0；该结果只证明参考文件覆盖，不证明 runtime 等价重建已完成。Batch 1 必须另行执行两个代表 seed + terminal weights 的真实重建并报告：候选深度、与 combined reference 的 Top20 overlap、rank correlation 和 score distribution shift，不得把参考 smoke 写死为成功。

### 5.2 正式候选

正式影子推理必须消费目标 Program 在 `target_trade_date` 已经存在的 `selection_effective` 候选及其 stage/component trace，并按 rank 取前 `Program.target_count` 行形成模型 group。它不得从 `prediction_ref_uri` 回放历史文件来替代当前候选，不得把训练文件最后一日冒充当前日期，也不得把用于持仓退出观察的更深 selection rows 纳入 Top20 reranker。

当前 Program 的 `target_count=20` 保持不变。模型 Top5 是 shortlist 派生视图，不改变现有 ENTER/HOLD/EXIT、每日替换预算或 active pool。候选 group key 固定为 `(program_id, binding_version_id, package_id, manifest_sha256, decision_as_of_trade_date, target_trade_date, selection_runtime_semantics_hash)`。

### 5.3 单 Alpha 兼容

单 Alpha Program 使用同一 `CandidateGroupV1` envelope：`component_count=1`、`combined_score=component_score`、`component_weight=1.0`。但本切片冻结的 SHORT_REBOUND 多 Alpha `AdvisoryFeatureSchemaV1` 实例要求两腿字段，不能用于单 Alpha。未来单 Alpha 基于自身历史预测训练时生成独立的 package/style/schema hash 和组件列集合；不得用 `not_applicable`、零值或复制单腿来满足当前两腿 schema。

模型 bundle selector 必须至少绑定 `package_id/manifest_sha256/style_profile/feature_schema_hash`。本切片训练的 SHORT_REBOUND 多 Alpha bundle 不得应用到当前风格未分类的单 Alpha 包 `pkg_378eb9c91e104c64935404e257e932ee`。单 Alpha API 合同必须正常工作，但在没有匹配 bundle 时于读取市场特征前返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`；后续只有基于该单 Alpha 自身历史预测完成训练后，才可返回其真实 model rank。

## 6. Feature Contract

### 6.1 共享 schema

`AdvisoryFeatureSchemaV1` 冻结每列的 name、dtype、unit、lookback、missing policy 和 source role。离线与正式推理调用同一个纯函数 `SharedAdvisoryFeatureBuilderV1.build(group, feature_snapshot)`；只有 `FeatureSource` adapter 不同。

必需身份列：

```text
program_id, binding_version_id, package_id, manifest_sha256,
decision_as_of_trade_date, target_trade_date, symbol,
selection_source_rank, selection_effective_rank, combined_score,
candidate_group_size, alpha_mode, selection_runtime_semantics_hash,
feature_schema_version
```

首模逐列 formula registry 冻结如下。所有窗口均以 `decision_as_of_trade_date=d` 为末端，`h` 表示交易日；`adj_x=raw_x*factor`，金额先按现有 QE 单位合同统一后再计算。滚动窗口不满足表中最少观测数时保留 NaN 和 missing indicator，不缩短窗口冒充完整值。

| 输出列 | 精确公式或来源 | 最少观测/类型 |
|---|---|---|
| `parent_combined_score` | 当前 runtime 两腿 normalized score 按 terminal weights 加权和 | required float |
| `parent_rank_pct` | `1-(selection_effective_rank-1)/max(candidate_group_size-1,1)` | required float |
| `lstm_raw_score/lstm_norm_score/lstm_leg_rank/lstm_weight` | LSTM 代表模型的正式 component fields | required float/int |
| `fund_raw_score/fund_norm_score/fund_leg_rank/fund_weight` | FUNDGROWTH 代表模型的正式 component fields | required float/int |
| `leg_norm_score_gap` | `lstm_norm_score-fund_norm_score` | required float |
| `leg_rank_gap` | `lstm_leg_rank-fund_leg_rank` | required int |
| `leg_direction_agreement` | 两腿 normalized score 同号为 1，否则 0 | required int |
| `weight_concentration` | `lstm_weight^2+fund_weight^2` | required float |
| `ret_{1,3,5,10,20}` | `adj_close[d]/adj_close[d-h]-1` | `h+1` 个交易日 |
| `drawdown_{20,60}` | `adj_close[d]/max(adj_high[d-h+1:d])-1` | `h` 个交易日 |
| `atr14_close` | 14 日 `max(high-low,abs(high-prev_close),abs(low-prev_close))` 均值除以 `adj_close[d]` | 15 个交易日 |
| `intraday_range` | `(adj_high[d]-adj_low[d])/adj_close[d]` | decision day |
| `open_gap` | `adj_open[d]/adj_close[d-1]-1` | 2 个交易日 |
| `volume_ratio_{5,20}` | `volume[d]/mean(volume[d-h+1:d])`，分母不大于 0 时 NaN | `h` 个交易日 |
| `amount_ratio_{5,20}` | `amount[d]/mean(amount[d-h+1:d])`，分母不大于 0 时 NaN | `h` 个交易日 |
| `turnover_rate/quoted_volume_ratio` | `db_turnover_rate/db_volume_ratio` 的 `d` 日值 | optional float |
| `main_net_amt_ratio` | `(mf_lg_buy_amt+mf_elg_buy_amt-mf_lg_sell_amt-mf_elg_sell_amt)/amount` | decision day；amount>0 |
| `elg_net_amt_ratio` | `(mf_elg_buy_amt-mf_elg_sell_amt)/amount` | decision day；amount>0 |
| `main_net_amt_ratio_{5,20}` | 同窗口 `sum(main_net_amt)/sum(amount)` | `h` 日且分母>0 |
| `elg_net_amt_ratio_{5,20}` | 同窗口 `sum(elg_net_amt)/sum(amount)` | `h` 日且分母>0 |
| `value_pe_inv` | `1/db_pe_ttm`，仅 `db_pe_ttm>0` | optional float |
| `value_pb_inv` | `1/db_pb`，仅 `db_pb>0` | optional float |
| `size_log_mv` | `log1p(max(db_circ_mv,0))`，两 adapter 先统一为 QE `db_circ_mv` 单位 | optional float |
| `revenue_yoy/profit_yoy/gross_margin/net_margin` | `bb_rev_yoy/bb_profit_yoy/bb_gpr/bb_npr` | optional float |
| `chip_winner_rate` | `cp_winner_rate` | optional float |
| `chip_cost_spread` | `(cp_cost_95pct-cp_cost_5pct)/adj_close[d]` | optional float；close>0 |
| `chip_cost_position` | `(adj_close[d]-cp_cost_50pct)/adj_close[d]` | optional float；close>0 |
| `margin_balance_log` | `log1p(max(md_rzye,0))` | optional float |
| `margin_balance_change_5` | `md_rzye[d]/md_rzye[d-5]-1`，历史值大于 0 | 6 个有效交易日 |
| `l2_code_id` | `sector_data.l2_code_id` | LightGBM categorical；train vocabulary 外或缺失均为 NaN |
| `sector_ret_{1,5,20}` | `sw2_close[d]/sw2_close[d-h]-1` | `h+1` 个交易日 |
| `sector_excess_{5,20}` | `sector_ret_h-csi300_ret_h` | 两侧均可用 |
| `sector_amount_ratio_20` | `sw2_amount[d]/mean(sw2_amount[d-19:d])` | 20 日；分母>0 |
| `sector_net_amt_ratio` | `sw2_mf_net_amt[d]/sw2_amount[d]` | decision day；分母>0 |
| `csi300_ret_{1,5,20}` | `000300.SH close[d]/close[d-h]-1` | `h+1` 个交易日 |
| `market_up_ratio` | `d` 日 PIT universe 中 `adj_close[d]>adj_close[d-1]` 的有效成员比例 | 至少 100 个成员 |
| `market_limit_up_ratio` | `d` 日 PIT universe 中 Qlib Bin 真实 `limit_up` flag 比例 | 至少 100 个成员 |
| `market_cross_section_vol` | `d` 日 PIT universe 个股 1 日收益 sample std，`ddof=1` | 至少 100 个成员 |
| `hmm_bull_posterior/hmm_state/hmm_state_duration/hmm_observation_completeness` | §7 冻结 HMM bundle 对候选 `d` 日 L2 行业的因果输出 | HMM 可用时；缺失显式 NaN/indicator |
| `decision_is_suspended/decision_limit_up/decision_limit_down` | suspend sidecar 与 Qlib Bin 在 `d` 日的真实状态 | required bool/int |
| `distance_to_limit_up/down` | `(adj_limit_price-adj_close[d])/adj_close[d]`，`adj_limit_price=raw_limit_price*factor[d]` | decision day；limit price 与 close>0 |

38-seed mean/std/min/max 不属于 `AdvisoryFeatureSchemaV1`。`selection_effective_rank` 和父包 component rank 可以作为输入特征，但不得进入标签 tie-break。每个 optional 输出列确定性配套一个 `<feature_name>__missing` bool；不存在运行时自由命名的 indicator。required 列缺失令 group typed failure，不生成 indicator 后继续。

### 6.2 Missing policy

- 身份、双日期、runtime semantics、候选分数、rank、OHLC、label price 或日期缺失：该 group 以 typed error 退出模型通道。
- 允许缺失的基本面、资金、融资和筹码字段：保留 NaN，并生成对应 missing indicator。
- 不允许按日动态删列、用 0 填充未知语义、换用其它数据源或缩减 feature set 后继续标记成功。
- M0 必须先报告每列 coverage。某一 optional 列在 train 中全空时不得运行时动态删列：在训练开始前修订 `feature_schema_version` 明确移除该列，或保留 NaN 并将其从 LightGBM feature list 显式排除；两种处置都写入 request，不形成审批或历史证据任务。

### 6.3 文件与数据库 parity

`QEFileFeatureSource` 与 `DatabaseRealtimeFeatureSource` 输出同一个 `RawAdvisoryFeatureSnapshotV1`。实现必须提供 schema parity 测试，对同一历史 `decision_as_of_trade_date`、同一候选集合比较列名、dtype、单位、公式和 source business-date cutoff；仅允许已报告的数据 vintage 导致的值差异，不允许公式或日期偏移分叉。

所有正式查询满足 `source_business_date<=decision_as_of_trade_date`，不得读取 `target_trade_date` 行。日线、daily_basic、moneyflow、bak_basic、margin、cyq 和 sector 数据均取不晚于 `d` 的最新合法业务行；是否允许 as-of 取最近行由各列 schema 固定，OHLC/limit/suspend/market breadth 必须是 `d` 日精确行，不得前向填充。数据库没有 `published_at` 的数据不能冒充盘中可用，本切片只支持决策日收盘后影子推理。

数据库 adapter 必须按日期和 symbol 集合批量查询，禁止每只股票或每列单独访问数据库。它读取已有 market 表和当前 Program/Selection 输入，不建立训练缓存表或 DDL。首版字段映射冻结为：

| 文件逻辑源 | 正式数据库源 |
|---|---|
| `daily_pv` / limit fields | `market.kline_daily_raw` |
| `daily_basic` | `market.daily_basic` |
| `moneyflow` | `market.moneyflow_ts` |
| `bak_basic` | `market.bak_basic` |
| `margin_detail` | `market.margin_detail` |
| `cyq_perf` | `market.cyq_perf` |
| `sector_data` / L2 mapping | `market.sector_data` 及现有 PIT 申万成员映射 |
| CSI300 | `market.index_daily` 的 `000300.SH` |
| suspend | `market.suspend_d` 的 `suspend_type='S'` |

优先复用 `backend/data_service/qe_data_service.py` 中现有批量查询和单位映射，但不得复用其历史数据库导出入口来训练模型。正式 adapter 必须显式投影字段并保持 H5 的 `db_`、`mf_`、`bb_`、`md_`、`cp_` 和 `sw2_` 命名合同。`l2_code_id` 使用 train 区间冻结的 categorical vocabulary；未见类别保持 NaN 和 missing indicator，不映射为某个已知行业。

## 7. Fresh HMM Contract

新 HMM 保留现有行业两状态 Gaussian HMM 与因果 forward-filter 思路，但数据 adapter 改为文件读取：

| 项目 | v1 冻结值 |
|---|---|
| sector level | SW L2 |
| states | 2 |
| covariance | full |
| random_state | 42 |
| min trading days | 120 |
| observations | sector daily return、20d excess return vs CSI300、sector volume share、真实 limit-up ratio |
| limit-up source | Qlib Bin `limit_up`，禁止 9.8% 近似 |
| fit period for holdout | 只使用 train `2024-07-04..2025-07-09` |
| validation/test inference | 固定模型参数，逐日 forward-filter，只使用截至该日观测 |
| observation transform | 每个 L2 行业按 train 拟合均值/标准差；零方差或非有限列令该行业 unavailable，不用全市场统计替代 |
| state canonicalization | 按拟合状态的 sector excess-return 均值升序固定 `state=0 BEAR/1 BULL`；禁止依赖 hmmlearn 原始标签顺序 |
| final shadow model | 使用 holdout 评估时同一组 train-fitted 参数，不在不重训 reranker 的情况下单独 refit HMM |
| continuation cutoff | 用固定参数顺序 forward-filter 文件观测至候选/模型共同连续截止 `2026-03-10`，保存各行业最后 posterior/state/duration/date；正式推理从 `2026-03-11` 起只追加数据库后续观测 |

HMM 四维 observation 公式固定为：`sector_return_1=sw2_close[d]/sw2_close[d-1]-1`；`sector_excess_20=sector_return_20-csi300_return_20`；`sector_amount_share=sw2_amount[d]/sum(all valid SW L2 sw2_amount[d])`；`sector_limit_up_ratio=count(PIT L2 members with true Qlib limit_up[d])/count(PIT L2 members with valid limit flag[d])`。分母为 0、有效成员少于 5 或任一维缺失时该 sector-date observation unavailable，不用 9.8% 或全市场值替代。

`fresh_hmm_models.json` 必须逐行业保存 observation order、train transform statistics、transition/start probability、means/covariances、canonical state mapping、fit range、文件续推截止日 posterior、state duration 和最后观测日。bundle hash 覆盖这些字段。真实数据核对发现多 Alpha 候选及所有行业共同有效 HMM observation 均截止于 `2026-03-10`，`2026-03-11..2026-06-30` 不存在可用于该候选模型的共同连续 observation；因此不得仅因基础行情文件更新到 `2026-06-30` 就把 HMM continuation cutoff 推迟到该日。数据库续推必须从 `2026-03-11` 按交易日连续追加；若交易日有缺口、行业映射未知或 observation 不完整，该行业返回 typed unavailable，不得跳日后假装连续状态。

旧 `SectorHMMTrainer` 的算法和参数序列化可以复用，数据库查询、旧 model JSON、旧状态序列和 neutral fallback 不可复用。任一 sector 失败必须记录 sector/reason；覆盖不足的 sector HMM 特征为 unavailable，不静默使用旧系数。M1 只有在新 HMM bundle 非空、test 期产生因果状态且从 `2026-03-10` continuation state 可确定性续推时才算完成。未来若获得 `2026-03-11` 之后完整且连续的 HMM observation 文件并决定重拟合 HMM，必须同步重建全部 reranker 特征并重训 reranker，作为新 bundle 版本；禁止只替换 HMM 文件。

## 8. Label And Split Contract

### 8.1 时间切分

406 个 decision dates 按时间顺序冻结为：

| split | 日期数 | 范围 |
|---|---:|---|
| train | 246 | `2024-07-04..2025-07-09` |
| purge-1 | 10 | `2025-07-10..2025-07-23` |
| validation | 60 | `2025-07-24..2025-10-23` |
| purge-2 | 10 | `2025-10-24..2025-11-06` |
| test | 80 | `2025-11-07..2026-03-10` |

同一 decision date 的全部股票属于同一 split。禁止随机切分。purge 固定为 10 个交易日，以覆盖 5 日 nominal horizon 加最多 5 日延迟退出；任一 label 的 `actual_exit_date` 越过其 split 后续 purge 边界时，该样本从拟合/评估排除并报告。父包最后 decision date `2026-03-10` 的 nominal 标签在 `2026-03-17` 成熟，最迟允许退出日在其后第 5 个交易日，仍早于基础数据截止 `2026-06-30`，因此 test 尾部无需因文件截止删减。

### 8.2 5 日主标签

```text
decision_as_of_trade_date = d
target_trade_date = next trading day after d
entry_date = target_trade_date
entry_price = entry_date adjusted open when executable
nominal_exit_date = fifth trading day after d
actual_exit_date = nominal_exit_date, or first executable close in the next 5 trading days
exit_price = actual_exit_date adjusted close
open_cost = 0.000095
close_cost = 0.000595
stock_net_return_5 = exit_price * (1-close_cost) / (entry_price * (1+open_cost)) - 1
benchmark_return_5 = CSI300_close[actual_exit_date] / CSI300_open[entry_date] - 1
excess_return_5 = stock_net_return_5 - benchmark_return_5
path_mfe_5 = max(0, max(adj_high[entry_date:actual_exit_date] * (1-close_cost) / (entry_price * (1+open_cost)) - 1))
path_mae_loss_5 = max(0, -min(adj_low[entry_date:actual_exit_date] * (1-close_cost) / (entry_price * (1+open_cost)) - 1))
utility_5 = excess_return_5 + 0.25 * path_mfe_5 - 0.50 * path_mae_loss_5
```

entry day停牌或一字涨停时标记 `NO_EXECUTABLE_ENTRY`，不进入主排序标签，同时进入可成交性 coverage。一字涨停精确定义为真实 `limit_up=true` 且 `raw_low>=raw_limit_up_price`；一字跌停精确定义为真实 `limit_down=true` 且 `raw_high<=raw_limit_down_price`。nominal exit day停牌或一字跌停时，从随后最多 5 个交易日寻找第一个非停牌且非一字跌停的收盘；仍不可执行则标记 `RIGHT_CENSORED_EXIT`，不进入主标签。延迟退出时股票、benchmark 和 MFE/MAE 使用相同 `actual_exit_date`，并保存 `actual_holding_trading_days`；不得比较不同暴露区间。复权因子必须在 entry/exit/path 全程一致，不使用回测持仓、分钟成交或未来状态做特征。本标签按比例成本计算，不假设下单金额，因此不引入依赖名义本金的最低 5 元佣金；该选择写入 `label_policy.json`，不能由实现临时切换。

每个 group 内先对不同 `utility_5` 值做 ascending dense rank。设不同值数量为 `m`、当前从 0 开始 dense rank 为 `d`：`m>1` 时 `relevance=floor(4*d/(m-1))`；`m=1` 时全部 relevance 为 0 并标记 `NO_LABEL_VARIATION`，不进入拟合。相同 utility 必须得到相同 relevance，symbol 和原始 selection rank 只用于稳定序列化，不参与标签 tie-break。LightGBM 固定 `label_gain=[0,1,3,7,15]`。主拟合 group 至少需要 5 个成熟且可执行 label；不足 group 保留 coverage 但不进入拟合。该 relevance 只用于排序，不得显示为收益概率。

### 8.3 基线与指标

必须同时报告：

- 原始 `selection_effective_rank` Top5。
- fresh HMM state posterior 排序 Top5，仅作独立对照。
- LambdaRank Top5。
- 固定 seed 的随机5。
- Top20 等权。

主指标为 date-level NDCG@5、Top5 mean excess return、`stock_net_return_5>0` 的 absolute hit rate、`excess_return_5>0` 的 excess hit rate、path MFE/MAE、相邻 shortlist turnover 和 modelable date coverage。随机5按每个 test date 使用 `seed=20260808` 无放回抽样；Top20 等权报告全部可执行候选的同口径均值，不伪装为 Top5 baseline。test 只有 80 日，结果必须标记 `RETROSPECTIVE_HOLDOUT/EXPERIMENTAL_SHADOW/UNCALIBRATED`，不得冒充 package 上线后的 formal forward OOS，也不设置阻止用户查看真实结果的收益阈值。

## 9. WSL Training Contract

### 9.1 入口

计划入口：

```text
scripts/advisory_model_prepare_request.py
scripts/advisory_model_train_wsl.py
scripts/wsl/advisory_model_train.py
```

Windows launcher 必须生成 request JSON 后执行等价命令：

```bash
wsl bash -lc '
  source /home/lc999/miniconda3/etc/profile.d/conda.sh &&
  conda activate rdagent-gpu &&
  export MALLOC_ARENA_MAX=2 &&
  export PYTHONUNBUFFERED=1 &&
  cd <explicit-wsl-repository-root> &&
  python scripts/wsl/advisory_model_train.py --request <explicit-request-path>
'
```

不得在 WSL 路径缺失时转到 Windows Python。repository root、request、output root 和数据根必须显式传入；实现不得猜测其它 worktree、RD-Agent workspace 或 historical artifact root。

### 9.2 模型配置

首模使用 LightGBM `objective=lambdarank`、`metric=ndcg`、`eval_at=[5]`：

```text
num_leaves=31
learning_rate=0.03
n_estimators=600
min_data_in_leaf=40
feature_fraction=0.8
bagging_fraction=0.8
bagging_freq=1
lambda_l1=0.1
lambda_l2=1.0
early_stopping_rounds=60
label_gain=[0,1,3,7,15]
deterministic=true
force_col_wise=true
num_threads=4
seed=20260808
feature_fraction_seed=20260808
bagging_seed=20260808
data_random_seed=20260808
```

`l2_code_id` 通过冻结 train vocabulary 作为 LightGBM categorical feature；其余字段保持 float/int 与 NaN，不执行全样本 scaler、全区间 imputation 或 test-fitted transform。输入按 `(decision_as_of_trade_date, selection_effective_rank, symbol)` 稳定排序，group sizes 与该顺序逐项校验。

validation 决定 best iteration，test 不参与训练、early stop、feature selection 或阈值选择。本切片不做超参搜索；训练跑通后 M5 再增加窗口、seed 和校准。发布给影子推理的 reranker 就是产生该 test report 的同一模型，不在评估后静默用 test 或全量数据 refit；任何后续 refit 都必须生成新 bundle 和新 retrospective holdout 报告。

### 9.3 资源与性能

- 单进程 RSS 必须低于 8GB；超限以 `ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED` 终止并输出阶段/峰值。
- 38 个 prediction 按 run 流式读取后只保留候选日期和需要列；不同时常驻 38 个完整 DataFrame。
- H5/Parquet 按日期、symbol 和列投影分批读取；临时训练矩阵按月写 Parquet shard。
- 临时 shard 属于本次训练工作目录，成功或失败均可诊断；不建设 SQLite 或长期缓存平台。
- 首个真实训练目标在 1 小时内完成。超过目标先记录各阶段 wall time、RSS 和 I/O，优化批量读取，不扩建基础设施。

## 10. Model Bundle Contract

唯一新增运行配置为 `AISTOCK_ADVISORY_MODEL_ROOT`，它只表示模型文件根，不是审批或业务门禁。未配置时模型 API 返回 `ADVISORY_MODEL_ROOT_NOT_CONFIGURED`，规则荐股继续运行。

`FrozenAdvisoryTrainingRequestV1` 至少冻结：

```text
request_id / request_sha256 / created_at
package_id / manifest_sha256 / package_asset_closure_hash
style_profile_id / style_profile_hash / effective_package_oos_cutoff
selection_runtime_semantics_id / selection_runtime_semantics_hash
multi_alpha_provider_version / normalization_method / raw_top_k / target_count
representative_seed_run_ids / representative_model_asset_sha256
full_seed_roster / prediction_manifest_sha256 / prediction_artifact_sha256
weight_policy_mode / terminal_weights
combined_reference_path / combined_reference_sha256 / diagnostic_only=true
qe_dataset_ids / qe_cutoff / hmm_continuation_cutoff / qe_schema_hashes / explicit WSL roots
decision_clock_version / feature_schema_version / label_policy_version
train-purge-validation-purge-test exact date lists
trainer parameters / output root / repository commit
```

`package_asset_closure_hash`、style profile 和 runtime semantics 只复制已存在的权威身份，不调用 package health、asset eligibility 或二次准入。QE dataset identity 使用已有版本目录名、cutoff 和 schema hash；不新增逐文件 source ledger、SEALED snapshot 或全 Bin 内容哈希。

所有 model-first request/schema/bundle JSON identity 使用现有纯函数 `backend.services.strategy_package.runtime_variant.canonical_json_sha256`，输入只允许有限 JSON 标量，不调用 `advisory_historical_range.canonical`。`request_sha256` 对 functional payload 计算，排除 `request_id/created_at/output_root`；`request_id=advmreq_<request_sha256前24位>`。同一 functional request 重跑必须得到同一 request identity，但本切片不为此建设 operation/lease/exact-retry 状态机。物理输出根变化不改变训练业务身份。

`AdvisoryModelBundleV1` 最少包含：

```text
manifest.json
model.txt
fresh_hmm_models.json
feature_schema.json
training_request.json
split.json
label_policy.json
metrics.json
test_predictions.parquet
baseline_comparison.json
training_log.json
```

manifest 保存 package/manifest/asset closure/style/runtime semantics、代表 seed、当前 normalization/terminal weights、完整 roster、38 个 prediction SHA、combined diagnostic SHA、QE dataset descriptors、双日期 clock、代码 commit、WSL/Conda/LightGBM版本、model SHA、HMM continuation-state SHA、训练日期和 retrospective test 状态。它不包含 Historical Range、SEALED、source revision 或 ModelOps 状态。

加载器必须验证 manifest schema、model SHA 和 feature schema hash。失败时返回 typed model error，不加载半包，不回退旧模型。

bundle identity 与发布路径固定为：

```text
bundle_id = sha256(canonical bundle manifest without bundle_id)
<root>/bundles/<bundle_id>/...
<root>/shadow_bindings/<package_id>/<manifest_sha256>/<style_profile_hash>.json
```

trainer 先在同一文件系统的临时目录写完整 bundle，逐文件读回并校验后原子 rename 到 `bundles/<bundle_id>`；最后原子替换唯一 shadow binding 文件。loader 只读取该 exact binding 中的 `bundle_id/bundle_manifest_sha256`，禁止扫描目录、按 mtime/latest 选择或在多个匹配 bundle 中猜测。训练成功和 shadow binding 激活分别报告，但不需要角色、审批或收益门槛；激活动作由明确的训练/发布命令参数执行。rollback 只移除该 exact shadow binding 或恢复一个明确 bundle id，不自动选择旧模型。

## 11. Realtime Inference And API

### 11.1 Service

计划模块：

```text
backend/services/advisory_model_first/contracts.py
backend/services/advisory_model_first/qe_file_source.py
backend/services/advisory_model_first/realtime_feature_source.py
backend/services/advisory_model_first/shared_feature_builder.py
backend/services/advisory_model_first/fresh_hmm.py
backend/services/advisory_model_first/labels.py
backend/services/advisory_model_first/reranker_training.py
backend/services/advisory_model_first/model_bundle.py
backend/services/advisory_model_first/model_inference.py
```

现有 `backend.services.advisory_modeling` 包及其中依赖 Historical Range/SEALED 的文件保持原样且不作为新入口。新实现使用独立 `backend.services.advisory_model_first` 包，任何新模块不得 import `backend.services.advisory_modeling`、`advisory_historical_range`、`advisory_phase1`、`base_snapshot.py`、`batch_b.py` 或 `feature_snapshot.py`。这样不会因旧 `advisory_modeling.__init__` 的 eager imports 形成传递依赖，也不需要修改旧包来解除依赖。

### 11.2 API

新增只读接口：

```text
GET /api/v1/advisory/programs/{program_id}/model-shadow?target_trade_date=YYYY-MM-DD
```

响应 `AdvisoryModelShadowResponseV1`：

```text
status = EXPERIMENTAL_SHADOW | MODEL_UNAVAILABLE
calibration_state = UNCALIBRATED
program_id / binding_version_id / package_id / manifest_sha256
decision_as_of_trade_date / target_trade_date
selection_runtime_semantics_hash / model_version / bundle_id / feature_schema_version
candidate_count / shortlist_count
candidates[]:
  symbol
  selection_effective_rank / score
  advisory_model_rank / score
  is_top5
  top_feature_contributions
baselines
reason_code / message
```

接口从目标日现有 Selection/Advisory date context 解析 `decision_as_of_trade_date`，并验证它早于 `target_trade_date`；数据库读取全部绑定前者。首个 shadow bundle 的 HMM continuation state 固定在 `2026-03-10`，正式推理必须从 `2026-03-11` 起按交易日用数据库 observation 连续推进到请求日；不能用训练文件中 `2026-06-30` 的行情截止替代这段连续状态。`decision_as_of_trade_date<=2026-03-10` 的历史结果读取 bundle 内 test report，不通过当前 API 反向运行 HMM，接口返回 `ADVISORY_MODEL_DECISION_BEFORE_CONTINUATION_CUTOFF`。接口不触发 Selection、review、active pool 或 list version 写入。若目标日没有权威 Selection 候选或双日期上下文，返回 `ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE` 或 `ADVISORY_MODEL_DECISION_CLOCK_MISMATCH`；不得现场回跑策略包或使用训练 PKL填充。`top_feature_contributions` 使用 LightGBM `pred_contrib`，按绝对值取前 5 并保留正负号，不用手写解释规则。

正式推理允许 `1<=candidate_count<=20`，输出 `shortlist_count=min(5,candidate_count)`，不跨日期补位。`ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE` 只表示身份缺失、重复 symbol、rank 不连续或 required feature 缺失，不得用于把合法浅 group 伪装成错误。

### 11.3 Page

`frontend/src/app/paper-v2/advisory/page.tsx` 增加模型影子视图：

- Program 切换保持多个包独立。
- 显示原排名、模型排名、Top5 标记和 test 基线摘要。
- 明确显示 `EXPERIMENTAL_SHADOW`、`UNCALIBRATED` 和错误 reason。
- 模型不可用时保留现有荐股页面和规则结果，不显示伪造模型列。
- 不恢复页面内手工多策略包融合。

## 12. Error Visibility

关键 reason codes：

```text
ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH
ADVISORY_MODEL_PREDICTION_MANIFEST_MISSING
ADVISORY_MODEL_PREDICTION_HASH_MISMATCH
ADVISORY_MODEL_REFERENCE_COMBINATION_MISMATCH
ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH
ADVISORY_MODEL_DECISION_CLOCK_MISMATCH
ADVISORY_MODEL_DECISION_BEFORE_CONTINUATION_CUTOFF
ADVISORY_MODEL_QE_SCHEMA_MISMATCH
ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE
ADVISORY_MODEL_LABEL_NOT_MATURE
ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING
ADVISORY_MODEL_HMM_TRAINING_FAILED
ADVISORY_MODEL_TRAINING_REQUIRES_WSL
ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED
ADVISORY_MODEL_BUNDLE_INVALID
ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE
ADVISORY_MODEL_ROOT_NOT_CONFIGURED
ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE
ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE
```

后台日志只在操作边界记录 package/program/date、阶段、reason、行数、耗时和峰值内存；禁止输出完整 DSN、密码、全量候选、每行成功日志或无上下文异常。

## 13. Implementation Plan / 实施顺序

### Batch 1：M0 请求与文件读取

1. 实现 `FrozenAdvisoryTrainingRequestV1`。
2. 实现精确 roster、两个代表 seed 训练输入、terminal weights/current normalization 和 `selection_runtime_semantics_hash`。
3. 读取其余 36 seed、combined prediction 和 scheme 32 逐日权重，生成不进入首模 feature 的对照诊断。
4. 实现 Qlib Bin、H5/Parquet 和 suspend sidecar adapter。
5. 生成 runtime-equivalent candidate group coverage、双日期和 schema report。

完成判定：406 个 decision dates 均按代表 seed + terminal weights 产生非空 `N<=20` candidate group，并报告 group depth、与 historical combined reference 的 overlap/rank/score shift；38 个 seed 和 combined diagnostic 的完整性单独报告且不进入在线 feature，已知当前资产应达到 38/38；不执行数据库训练读取或历史 DML。

### Batch 2：M1 Feature/HMM/训练

1. 实现共享 FeatureBuilder 与文件 FeatureSource。
2. 实现 fresh file HMM。
3. 实现标签与固定时间切分。
4. 实现 WSL launcher、LambdaRank trainer 和 bundle。
5. 执行真实训练并生成非空 80 日 test 预测。

完成判定：真实 model 和 fresh HMM continuation state 可加载；test Top5 非空；baseline comparison 非空；无旧 HMM 输入、无未来泄漏、无 Windows 训练，且 shadow 使用与 holdout 相同的 HMM/reranker 参数。

实际验收：`2026-08-08` 在 WSL `rdagent-gpu` 完成真实训练。406 日产生 8120 个 Top20 候选；fresh HMM 为 110 个可用行业、22 个明确 unavailable 行业；348 日特征 group 可用，标签 406 日可用；最终 train/validation/test 分别为 191/57/80 个模型可用日期，test 为 1599 行且 80 日均有 Top5。LightGBM best iteration 为 1，模型文件包含 103 个冻结特征并可读回。模型质量低于三个主要对照，因此 bundle 保持 `EXPERIMENTAL_SHADOW` 且不写 shadow binding；这不是新增收益门禁，而是对真实实验结果的准确状态描述。

### Batch 3：M2 数据库推理与页面

1. 实现数据库批量 FeatureSource。
2. 实现 model bundle loader 和 Program 级推理。
3. 增加只读 API。
4. 增加页面影子视图和浏览器验收。

完成判定：目标多 Alpha Program 从真实双日期 Selection 输入返回真实模型响应；单 Alpha Program 在没有匹配 bundle 时返回精确 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`，且不会误用多 Alpha 模型；两个 Program 的规则荐股均不受影响；Selection/Paper/模拟盘零写入。

Batch 1 完成后立即进入 Batch 2，不插入历史证据、归档或通用治理任务。

## 14. Design Acceptance Index

| ID | 设计要求 |
|---|---|
| F-301 | 精确绑定目标父包、manifest、asset closure、style、runtime semantics、Program 和 active binding，不做策略包二次准入 |
| F-302 | 首模候选使用两腿代表 seed + terminal weights/current normalization；完整 38 seed、逐日权重和 combined prediction 只作显式诊断，禁止伪 seed 特征、latest/扫描/跨语义替代 |
| F-303 | 训练基础数据只来自现有 QE H5/Parquet/Qlib 日线 Bin 和 suspend sidecar，不读取生产数据库历史数据 |
| F-304 | 历史候选按 decision/target 双日期复现正式代表模型 Top25，再取 Program target_count 前20；目标日停牌/涨跌停不得用于候选或特征 |
| F-305 | 离线与实时推理共享同一个 FeatureBuilder/schema、公式和 decision cutoff，只允许 FeatureSource adapter 不同 |
| F-306 | 首模逐列公式、单位、窗口、最少观测、categorical 和 missing policy 完整冻结；禁止伪 seed 离散度、动态删列或未知语义填充 |
| F-307 | HMM 使用当前文件数据从头拟合、状态确定性规范化、真实 limit flag、因果 forward-filter 和可续推 posterior；旧 HMM 仅作外部对照 |
| F-308 | 5 日标签、可执行 entry/actual exit、同期限 benchmark、成本、MFE/MAE、censor、relevance 和 label_gain 完整冻结 |
| F-309 | 406 decision dates 按 246/10/60/10/80 时间切分，10日 purge覆盖最长退出窗口，同日不跨 split，test 不参与选择 |
| F-310 | 使用真实 LightGBM LambdaRank，固定首模参数和 seed，不用规则/mock/随机冒充 |
| F-311 | 所有特征构建、HMM拟合和模型训练只在 WSL Conda执行，Windows不训练 |
| F-312 | 峰值内存低于8GB，批量/列投影/临时Parquet，目标小时级且不建SQLite或缓存平台 |
| F-313 | bundle 原子发布、exact binding 加载，并包含真实 model、fresh HMM continuation、schema、request、split、metrics 和非空 test predictions |
| F-314 | API只读消费 Program/Selection 的 decision/target context 和数据库 decision-cutoff 输入，缺输入不得回放历史 PKL 或重跑策略包 |
| F-315 | 页面展示真实 EXPERIMENTAL_SHADOW Top5、原排名、模型排名、基线和错误状态 |
| F-316 | 多个 Program 独立；单/原生多 Alpha 共用 CandidateGroup/API envelope，但 feature schema 与 bundle 必须匹配 package/manifest/style/schema，禁止跨包套用模型或恢复手工融合 |
| F-317 | 模型失败不阻断现有规则荐股，不用基线或旧模型冒充模型成功 |
| F-318 | Selection、Paper、模拟盘、QE和策略包业务逻辑零写入、零反向依赖 |
| F-319 | 不执行 Historical Range、Phase1R、SEALED/CAS、旧任务处理或历史证据工程 |
| F-320 | 不新增角色、审批、二次准入、收益门槛或未经确认的业务门禁 |
| F-321 | minute Bin不进入M0/M1/M2，首版价格范围后续只使用日线，盘中模型需用户另行确认 |
| F-322 | 所有失败使用 typed reason 和有效日志，不静默 fallback，不泄露敏感配置 |

## 15. Verification Plan

### 15.1 Batch 1

- 38/38 Prediction Store manifests 和 SHA 验证；两个代表 seed 必须与父包 runtime identity 一致。
- terminal weights/current normalization 重建 406 日 runtime-equivalent candidates；combined prediction 1,802,507 行、406 日只作 diagnostic reference。
- 406 个逐日权重和 historical combination parity、代表 seed 与 full ensemble 的 overlap/rank/score shift 报告。
- H5/Parquet/Bin/suspend schema 与日期覆盖。
- 每日 Top20 group 深度、decision/target 日期、runtime semantics 和缺失原因；断言 target-day 状态未进入候选。

### 15.2 Batch 2

- file/realtime raw snapshot 的 schema parity 单测。
- HMM train-only fit、state canonicalization、causal inference、文件截止 posterior、数据库连续续推和旧产物排除测试。
- label maturity、entry/exit、涨跌停、停牌和 censor 边界测试。
- relevance、label_gain、延迟退出同期限 benchmark 和 MFE/MAE 精确公式测试。
- split 不重叠、purge=10、最长退出窗口不跨边界、test immutable 测试。
- WSL identity、内存采样、训练失败 reason 和 bundle hash 测试。
- 真实 80 日 test predictions 与 5 组 baseline 报告。

### 15.3 Batch 3

- 目标多 Alpha Program 和一个单 Alpha Program API 行为测试；双日期/decision cutoff 错位必须 typed failure。
- 数据库查询次数随日期批次而非 symbol 数增长。
- 模型不可用仍返回规则荐股页面。
- API/UI 字段、状态、reason 和 Program 隔离。
- Playwright 在 375×812、768×1024、1440×900 验证无溢出、无重叠、无 console error。
- protected-module import/write scan。

### 15.4 DESIGN-COMPLIANCE-001

每次实现审核必须分别回答：

1. 是否存在简化版、subset、POC、mock-only、placeholder 或 static success。
2. 是否存在 silent fallback、异常吞噬、动态删列、旧模型回退或假成功。
3. 是否改变候选、标签、Program 隔离、数据来源或模块边界业务语义。
4. 是否新增用户未确认的门禁、审批、角色、二次准入或发布流程。

任一项缺少直接证据时不得报告完成或可合入。

## 16. Risks / 风险与直接处置

| 风险 | 直接处置 |
|---|---|
| 代表 Prediction Store run 缺失或 hash 不符 | 明确列出 run_id 并停止 runtime-equivalent candidate；不扫描 workspace、不换 latest |
| 非代表 Prediction Store run 缺失或 hash 不符 | full-ensemble diagnostic 显式 unavailable；不减 seed 后伪造完整诊断，但不阻断只依赖代表 seed 的首模训练 |
| historical combined 诊断重算不一致 | 报告最大误差和首个排序差异并标记 diagnostic failed；不把该参考替代 current runtime candidate，也不吞掉问题 |
| 代表 seed + terminal weights Top20 与正式 Selection 语义不一致 | 核对 provider version、normalization、runtime semantics、双日期和 Program 配置；不切换到 historical combined 或引入 Historical Range |
| H5/Parquet 列缺失或全空 | 报告精确列和覆盖；若确属非必要特征，先修订 design/schema 版本，不运行时动态删列 |
| fresh HMM 个别行业覆盖不足 | 记录行业 unavailable；不得复用旧 HMM；整体 HMM bundle 非空、state mapping稳定、test有因果输出且文件截止状态可续推才完成 M1 |
| 训练内存或耗时超限 | 用阶段 RSS/wall-time 定位后改为更窄列投影或更小日期 shard；不建设新缓存平台 |
| 正式数据库字段暂不可用 | 只补目标 market 查询/adapter；模型通道显式 unavailable，规则荐股继续运行 |
| 同一包出现多个 bundle | loader 只读 exact shadow binding；缺失或 hash 不符显式失败，禁止扫描 latest |
| 模型 test 表现不佳 | 如实展示实验结果并进入 M5 特征/窗口迭代；不把结果门槛变成用户不可见门禁 |
| 模型错误影响现有模块 | 保持 Advisory-only import 和只读 API；protected-module 测试阻止反向依赖或写入 |
| 工作再次偏向历史平台 | F-319 直接判定 scope drift，停止该变更并回到 M0/M1/M2 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-301 | `backend/services/advisory_model_first/target_binding.py`; `contracts.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; frozen request readback | verified | none |
| F-302 | `prediction_source.py`; `candidate_group.py`; `diagnostics.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; `test_review_regressions.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/runs/advmreq_ac5959aa8dc14a25e3b8c139/parent_diagnostics.json` | verified | none |
| F-303 | `qe_file_source.py`; `training_pipeline.py` | `backend/tests/advisory_model_first/test_qe_file_source.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/training_request.json` | verified | none |
| F-304 | `candidate_group.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/runs/advmreq_ac5959aa8dc14a25e3b8c139/candidate_coverage.json` | verified | none |
| F-305 | `shared_feature_builder.py`; `feature_schema_v1.py` | `backend/tests/advisory_model_first/test_shared_feature_builder.py` | design_ready | none |
| F-306 | `feature_schema_v1.py`; `reranker_training.py` | `backend/tests/advisory_model_first/test_shared_feature_builder.py`; `test_review_regressions.py` | verified | none |
| F-307 | `fresh_hmm.py` | `backend/tests/advisory_model_first/test_fresh_hmm.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/fresh_hmm_models.json` | verified | none |
| F-308 | `labels.py` | `backend/tests/advisory_model_first/test_labels.py`; `test_review_regressions.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/runs/advmreq_ac5959aa8dc14a25e3b8c139/label_coverage.json` | verified | none |
| F-309 | `time_split.py`; `labels.py` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/split.json` | verified | none |
| F-310 | `reranker_training.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/baseline_comparison.json`; `backend/tests/advisory_model_first/test_review_regressions.py` | verified | none |
| F-311 | `scripts/advisory_model_train_wsl.py`; `scripts/wsl/advisory_model_train.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/manifest.json`; `backend/tests/advisory_model_first/test_candidate_and_contracts.py` | verified | none |
| F-312 | projected reader；`training_pipeline.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/runs/advmreq_ac5959aa8dc14a25e3b8c139/training_receipt.json`; `backend/tests/advisory_model_first/test_qe_file_source.py` | verified | none |
| F-313 | `model_bundle.py` | `backend/tests/advisory_model_first/test_review_regressions.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/bundles/9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629/manifest.json` | design_ready | none |
| F-314 | planned `backend/services/advisory_model_first/model_inference.py` + router | `backend/tests/advisory_modeling/test_model_first_api_decision_clock.py` | design_ready | none |
| F-315 | planned Advisory page | `frontend/tests/advisory-model-first-shadow.spec.ts` | design_ready | none |
| F-316 | planned Program composition | `backend/tests/advisory_modeling/test_model_first_program_isolation.py` | design_ready | none |
| F-317 | planned inference error envelope | `backend/tests/advisory_modeling/test_model_first_baseline_continuity.py` | design_ready | none |
| F-318 | `backend/services/advisory_model_first/**` | `backend/tests/advisory_model_first/test_candidate_and_contracts.py`; changed-file review | verified | none |
| F-319 | dependency scan | `backend/tests/advisory_model_first/test_review_regressions.py`; no Historical Range/SEALED/source revision import | verified | none |
| F-320 | design/code review | `backend/tests/advisory_model_first/test_review_regressions.py`; no role/approval/package admission implementation | verified | none |
| F-321 | `qe_file_source.py`; frozen request | `backend/tests/advisory_model_first/test_qe_file_source.py`; minute Bin absent from request | verified | none |
| F-322 | `errors.py`; WSL driver；trainer typed errors | `backend/tests/advisory_model_first/test_review_regressions.py` | design_ready | none |

## 18. Rollout / Rollback

### 18.1 Rollout

1. 合入 Batch 1 源码后只生成训练 request 和 coverage，不启停用户服务。
2. 在 WSL 完成 Batch 2 真实训练，模型 bundle 保持 `EXPERIMENTAL_SHADOW`。
3. Batch 3 源码合入后，由用户决定并执行后端/前端重启。
4. 重启后只读核对源码身份、bundle identity、目标 Program API 和页面 readback。
5. 多 Alpha 通过后再用现有单 Alpha Program 验证相同合同；不修改两个 Program 的规则配置。

### 18.2 Rollback

- 源码回滚只移除模型 API/UI 消费入口，不回滚 Selection、Program、策略包或 QE 数据。
- 模型 bundle 异常时停止加载该 bundle并返回 typed unavailable；不删除模型文件，不自动切换旧模型。
- 移除或不配置 `AISTOCK_ADVISORY_MODEL_ROOT` 只关闭模型影子通道，现有规则荐股保持不变。
- 本切片无 DDL/DML，因此不存在数据库 rollback。

## 19. Production Gates / 生产影响（无新增业务门禁）

| 项目 | 当前设计结论 |
|---|---|
| production_ddl_gate | `noop`，本切片不需要新表 |
| production_dml_gate | `noop`，影子推理不写业务表 |
| production_backend_dependency_gate | 代码阶段按实际依赖变化单独报告；设计阶段 `noop` |
| production_frontend_dependency_gate | 代码阶段按实际依赖变化单独报告；设计阶段 `noop` |
| backend restart | 仅后续代码合入后由用户决定并执行 |
| runtime activation | 模型 root 配置和 bundle 加载独立报告，不与源码合入合并 |

## 20. 开工条件与下一步

M0/M1 源码、正式 WSL 训练和 bundle 读回已经完成。下一任务直接进入 Batch 3/M2：实现数据库 decision-cutoff FeatureSource、exact bundle loader、只读影子推理 API 和 Advisory 页面 readback；不得在此之前插入历史证据、归档、通用 ModelOps 或遗留任务处理。当前 bundle 质量低于对照，页面必须如实展示实验状态和 baseline，禁止描述为已校准或已优化。
