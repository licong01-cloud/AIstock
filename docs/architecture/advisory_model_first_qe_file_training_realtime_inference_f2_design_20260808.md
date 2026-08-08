# AIstock 荐股模型优先垂直切片 F2 详细设计 v1.0

> 日期：2026-08-08
> Feature tier：`F2`
> 当前状态：`DESIGN_READY_FOR_M0_M1_IMPLEMENTATION`
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v2.2
> 首个目标：原生多 Alpha 父包 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` 的 SHORT_REBOUND Top20→Top5 真实模型
> 训练边界：只在 WSL Conda 环境读取已有 QE H5/Parquet/Qlib Bin 和 Prediction Store PKL
> 推理边界：只在 Advisory 消费层读取数据库当前/实时输入，不修改 Selection、Paper、模拟盘或 QE

## 0. 文档定位与权威顺序

本文档落实父级蓝图 M0、M1 和 M2 的第一个可运行垂直切片。目标不是再建设训练数据平台，而是使用已经存在的 QE 文件，在 WSL 完成真实模型训练，再把真实模型结果接入 Advisory 页面。

发生冲突时按以下顺序处理：

1. 用户当前明确要求。
2. 父级蓝图 v2.2 的模型优先顺序、数据边界和模块隔离边界。
3. 本文档冻结的目标资产、候选、特征、标签、时间切分和 API 合同。
4. 当前源码中与以上内容不冲突的既有实现。

旧 `advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` 仅可作为公式和领域背景参考。其 Historical Range、Phase 1R、SEALED snapshot、CAS、source revision、capture/build bridge 和多年矩阵前置路径不得进入本切片实现。

## 1. Background / 已核实事实基线

### 1.1 目标父包与 Advisory Program

| 项目 | 冻结值 |
|---|---|
| package_id | `pkg_ma_8ec5e389fa2c5e484a1ac7e9` |
| manifest_sha256 | `f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016` |
| alpha_mode | `multi_alpha` |
| source run | `macb_7738e811293948eb_20240702_20260310_20260625T184334308696Z` |
| roster_hash | `7738e811293948eb` |
| combination | `ic_weighted`，按交易日 universe 归一化，weighted sum |
| parent raw candidate depth | `25`，与当前成功 selection artifact 的 `score_count/topk` 一致 |
| current Program | `advp_3126dd77f9774d94850f37ad012f640f` |
| active binding | `advb_f860140caa314665ad60ac089ed84b3f` |
| binding effective_from | `2026-07-20` |
| Program target_count | `20`，首个模型不得自动修改为 5 |
| runtime HMM/risk | 当前均 disabled；suspend filter enabled；industry blacklist 为空 |

目标包已进入系统并稳定产生选股结果。本切片不得调用 package health、asset eligibility、runtime admission 或其它二次准入逻辑来决定是否允许训练或推理。实现只读取上表已经显式冻结的包、manifest、Program 和 binding 身份；不重新验证策略包业务可用性。

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

38 个 run 的 Prediction Store manifest 均已只读核实存在且包含带 SHA256 的 `pred.pkl`。实现必须通过 `PredictionArtifactStore`/`ModelStoreService` 精确读取这些 manifest，不扫描 QE workspace，不使用 latest run，不在缺失时减少 seed 数。

### 1.3 合成预测与逐日权重

权威合成预测：

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

`combined_prediction.pkl` 是父包历史组合分数权威。各腿/各 seed prediction 用于组件特征和组合重算 parity；重算结果不得替代权威文件。若同一日期、symbol 的重算合成分数与权威合成分数最大绝对误差超过 `1e-8`，或候选排序不一致，该训练请求以 `ADVISORY_MODEL_PARENT_COMBINATION_MISMATCH` 失败，不静默选择任一版本。

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

1. 实现精确父包 roster 和预测文件读取器。
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
       -> 38 Prediction Store pred.pkl
       -> scheme 32 daily weights
       -> authoritative combined_prediction.pkl
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
Advisory Program + active binding
  -> existing Selection authoritative selection_effective candidates
  -> DatabaseRealtimeFeatureSource (batch query)
  -> fresh HMM model + DB observations through decision date
  -> SharedAdvisoryFeatureBuilderV1
  -> loaded AdvisoryModelBundleV1
  -> model score/rank + Top5
  -> Advisory API
  -> Paper v2 Advisory page / model shadow panel
```

模型服务只消费 Selection 已经生成的候选，不重新运行策略包推理，也不写回 Selection artifact。模型失败只让影子结果进入 `MODEL_UNAVAILABLE`，现有规则荐股继续返回原结果。

## 5. Contracts / Candidate Top20 身份

### 5.1 离线候选

历史候选阶段冻结为：

```text
OFFLINE_REPRODUCED_SELECTION_EFFECTIVE_TOP20_V1
```

逐交易日确定性步骤：

1. 读取权威 `combined_prediction.pkl` 当日全量 score。
2. 按 `score DESC, symbol ASC` 形成 raw rank，并先截取父包正式 runtime 产生的 raw Top25。
3. 只在该 raw Top25 内使用 suspend sidecar 排除当日 `suspend_type=S` 股票。
4. industry blacklist 为空，不做行业排除。
5. 当前父包 HMM/risk disabled，不对 score 二次乘权或过滤。
6. 取过滤后的前 20 并紧凑重排为 `selection_effective_rank=1..N`，其中 `N<=20`；若 Top25 内超过 5 只被排除，不得从 raw rank 26 以后补位。

训练候选不得从回测持仓或 Top25/Top50 portfolio result 提取。`filtered_pool_20260630`、PIT instrument ranges 和 suspend sidecar只负责复现与正式 Selection 相同的基础 eligibility，不生成新的研究准入。

设计阶段只读 smoke 已按上述顺序读取权威 combined prediction 和 suspend sidecar：406/406 个日期均得到深度 20，raw Top25 内停牌排除数均为 0。该结果证明当前真实数据可以通过候选合同，但代码阶段仍必须执行同一统计并输出 coverage，不得把本次人工 smoke 写死为成功。

### 5.2 正式候选

正式影子推理必须消费目标 Program 当日已经存在的 `selection_effective` 候选及其 stage/component trace。它不得从 `prediction_ref_uri` 回放历史文件来替代当前候选，不得把训练文件最后一日冒充当前日期。

当前 Program 的 `target_count=20` 保持不变。模型 Top5 是 shortlist 派生视图，不改变现有 ENTER/HOLD/EXIT、每日替换预算或 active pool。

### 5.3 单 Alpha 兼容

单 Alpha Program 使用同一 `CandidateGroupV1`：`component_count=1`、`combined_score=component_score`、`component_weight=1.0`。FeatureBuilder 不要求多 Alpha 专属字段非空；这些字段使用显式 `not_applicable` indicator，不以零伪装多 Alpha 证据。

模型 bundle selector 必须至少绑定 `package_id/manifest_sha256/style_profile`。本切片训练的 SHORT_REBOUND 多 Alpha bundle 不得应用到当前风格未分类的单 Alpha 包 `pkg_378eb9c91e104c64935404e257e932ee`。单 Alpha API 合同必须正常工作，但在没有匹配 bundle 时返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`；后续只有基于该单 Alpha 自身历史预测完成训练后，才可返回其真实 model rank。

## 6. Feature Contract

### 6.1 共享 schema

`AdvisoryFeatureSchemaV1` 冻结每列的 name、dtype、unit、lookback、missing policy 和 source role。离线与正式推理调用同一个纯函数 `SharedAdvisoryFeatureBuilderV1.build(group, feature_snapshot)`；只有 `FeatureSource` adapter 不同。

必需身份列：

```text
program_id, package_id, manifest_sha256, trade_date, symbol,
selection_effective_rank, combined_score, candidate_group_size,
alpha_mode, feature_schema_version
```

首模特征组：

1. **父包/组件**：combined score/rank/percentile、两腿 ensemble score/rank、38 个 seed score 的腿内 mean/std/min/max、腿间 score/rank 差、逐日权重、权重集中度、组件一致方向比例。
2. **量价**：1/3/5/10/20 日收益、20/60 日高点回撤、ATR14/close、日内振幅、gap、volume/amount 5/20 日比率、换手率和 volume ratio。
3. **资金与估值**：主力/超大单净流占比及 5/20 日累计、PE/PB inverse、市值对数、流动性。
4. **筹码与融资**：winner rate、成本分位差、融资余额与变化率。
5. **行业**：L2 code、行业 1/5/20 日收益、相对沪深300收益、行业成交/资金扩散。
6. **市场**：沪深300 1/5/20 日收益、横截面上涨比例、真实涨停比例、市场波动。
7. **fresh HMM**：sector state posterior、state label、state duration、observation completeness；不把 HMM coefficient 再乘到 score。
8. **可交易性**：decision day suspend/limit flags 和距涨跌停价距离；未来日状态不得进入特征。

### 6.2 Missing policy

- 身份、候选分数、rank、OHLC、label price 或日期缺失：该 group 以 typed error 退出模型通道。
- 允许缺失的基本面、资金、融资和筹码字段：保留 NaN，并生成对应 missing indicator。
- 不允许按日动态删列、用 0 填充未知语义、换用其它数据源或缩减 feature set 后继续标记成功。
- 若某一 optional 列在 train 中全空，训练请求失败并报告列名；必须修改正式设计版本后才能删列。

### 6.3 文件与数据库 parity

`QEFileFeatureSource` 与 `DatabaseRealtimeFeatureSource` 输出同一个 `RawAdvisoryFeatureSnapshotV1`。实现必须提供 schema parity 测试，对同一历史日期、同一候选集合比较列名、dtype、单位和公式；仅允许数据 vintage 导致的值差异，不允许公式分叉。

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

优先复用 `backend/data_service/qe_data_service.py` 中现有批量查询和单位映射，但不得复用其历史数据库导出入口来训练模型。正式 adapter 必须显式投影字段并保持 H5 的 `db_`、`mf_`、`bb_`、`md_`、`cp_` 和 `sw2_` 命名合同。

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
| fit period for holdout | 只使用 train `2024-07-04..2025-07-23` |
| validation/test inference | 固定模型参数，逐日 forward-filter，只使用截至该日观测 |
| final shadow model | 使用文件数据截至 `2026-06-30` 重拟合；正式推理只追加数据库后续观测 |

旧 `SectorHMMTrainer` 的算法和参数序列化可以复用，数据库查询、旧 model JSON、旧状态序列和 neutral fallback 不可复用。任一 sector 失败必须记录 sector/reason；覆盖不足的 sector HMM 特征为 unavailable，不静默使用旧系数。M1 只有在新 HMM bundle 非空且 test 期产生因果状态时才算完成。

## 8. Label And Split Contract

### 8.1 时间切分

406 个候选日期按时间顺序冻结为：

| split | 日期数 | 范围 |
|---|---:|---|
| train | 256 | `2024-07-04..2025-07-23` |
| purge-1 | 5 | `2025-07-24..2025-07-30` |
| validation | 60 | `2025-07-31..2025-10-30` |
| purge-2 | 5 | `2025-10-31..2025-11-06` |
| test | 80 | `2025-11-07..2026-03-10` |

同一交易日全部股票属于同一 split。禁止随机切分。父包最后候选日 `2026-03-10` 的 5 日标签在日线 Bin 的 `2026-03-17` 成熟，早于基础数据截止 `2026-06-30`，因此 test 尾部无需删减。

### 8.2 5 日主标签

```text
decision_date = t
entry_date = next trading day after t
entry_price = entry_date open when executable
nominal_exit_date = fifth trading day after t
exit_price = nominal_exit_date close when executable
open_cost = 0.000095
close_cost = 0.000595
stock_net_return_5 = exit_price / entry_price - 1 - open_cost - close_cost
benchmark_return_5 = CSI300_close[t+5] / CSI300_open[t+1] - 1
excess_return_5 = stock_net_return_5 - benchmark_return_5
utility_5 = excess_return_5 + 0.25 * MFE_5 - 0.50 * abs(MAE_5)
```

entry day停牌或一字涨停时标记 `NO_EXECUTABLE_ENTRY`，不进入主排序标签，同时进入可成交性 coverage。nominal exit day停牌或一字跌停时，从随后最多 5 个交易日寻找第一个可执行收盘；仍不可执行则标记 `RIGHT_CENSORED_EXIT`，不进入主标签。不得用未来状态做特征。

每个交易日内按 `utility_5` dense rank 映射 relevance `0..4`，同 utility 同 relevance。主拟合 group 至少需要 5 个成熟且可执行 label；不足 group 保留 coverage 但不进入拟合。该 relevance 只用于排序，不得显示为收益概率。

### 8.3 基线与指标

必须同时报告：

- 原始 `selection_effective_rank` Top5。
- fresh HMM state posterior 排序 Top5，仅作独立对照。
- LambdaRank Top5。
- 固定 seed 的随机5。
- Top20 等权。

主指标为 date-level NDCG@5、Top5 excess return、positive-return hit rate、MFE/MAE、turnover 和 modelable date coverage。test 只有 80 日，结果必须标记 `EXPERIMENTAL_SHADOW/UNCALIBRATED`，不设置阻止用户查看真实结果的收益阈值。

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
deterministic=true
force_col_wise=true
num_threads=4
seed=20260808
```

validation 决定 best iteration，test 不参与训练、early stop、feature selection 或阈值选择。本切片不做超参搜索；训练跑通后 M5 再增加窗口、seed 和校准。

### 9.3 资源与性能

- 单进程 RSS 必须低于 8GB；超限以 `ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED` 终止并输出阶段/峰值。
- 38 个 prediction 按 run 流式读取后只保留候选日期和需要列；不同时常驻 38 个完整 DataFrame。
- H5/Parquet 按日期、symbol 和列投影分批读取；临时训练矩阵按月写 Parquet shard。
- 临时 shard 属于本次训练工作目录，成功或失败均可诊断；不建设 SQLite 或长期缓存平台。
- 首个真实训练目标在 1 小时内完成。超过目标先记录各阶段 wall time、RSS 和 I/O，优化批量读取，不扩建基础设施。

## 10. Model Bundle Contract

唯一新增运行配置为 `AISTOCK_ADVISORY_MODEL_ROOT`，它只表示模型文件根，不是审批或业务门禁。未配置时模型 API 返回 `ADVISORY_MODEL_ROOT_NOT_CONFIGURED`，规则荐股继续运行。

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
training_log.jsonl
```

manifest 保存 package/manifest/combine run/roster、38 个 prediction SHA、combined prediction SHA、数据 snapshot ids、代码 commit、WSL/Conda/LightGBM版本、model SHA、HMM SHA、训练日期和 test 状态。它不包含 Historical Range、SEALED、source revision 或 ModelOps 状态。

加载器必须验证 manifest schema、model SHA 和 feature schema hash。失败时返回 typed model error，不加载半包，不回退旧模型。

## 11. Realtime Inference And API

### 11.1 Service

计划模块：

```text
backend/services/advisory_modeling/qe_file_source.py
backend/services/advisory_modeling/realtime_feature_source.py
backend/services/advisory_modeling/shared_feature_builder.py
backend/services/advisory_modeling/fresh_hmm.py
backend/services/advisory_modeling/reranker_training.py
backend/services/advisory_modeling/model_bundle.py
backend/services/advisory_modeling/model_inference.py
```

现有 `advisory_modeling` 中依赖 Historical Range/SEALED 的文件保持原样且不作为新入口。新模块不得 import `advisory_historical_range`、`advisory_phase1`、`base_snapshot.py`、`batch_b.py` 或 `feature_snapshot.py`。

### 11.2 API

新增只读接口：

```text
GET /api/v1/advisory/programs/{program_id}/model-shadow?trade_date=YYYY-MM-DD
```

响应 `AdvisoryModelShadowResponseV1`：

```text
status = EXPERIMENTAL_SHADOW | MODEL_UNAVAILABLE
calibration_state = UNCALIBRATED
program_id / binding_version_id / package_id / manifest_sha256
trade_date / model_version / feature_schema_version
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

接口不触发 Selection、review、active pool 或 list version 写入。若目标日没有权威 Selection 候选，返回 `MODEL_SELECTION_INPUT_UNAVAILABLE`；不得现场回跑策略包或使用训练 PKL填充。

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
ADVISORY_MODEL_PARENT_COMBINATION_MISMATCH
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
2. 实现精确 roster、38 manifest、combined prediction、scheme 32 权重读取。
3. 实现 Qlib Bin、H5/Parquet 和 suspend sidecar adapter。
4. 生成 candidate group coverage 和 schema report。

完成判定：406 个日期均产生非空 `N<=20` candidate group，并报告 group depth 分布和 Top25 内排除数；38 个 seed 和 combined prediction 均精确绑定；不执行数据库训练读取或历史 DML。

### Batch 2：M1 Feature/HMM/训练

1. 实现共享 FeatureBuilder 与文件 FeatureSource。
2. 实现 fresh file HMM。
3. 实现标签与固定时间切分。
4. 实现 WSL launcher、LambdaRank trainer 和 bundle。
5. 执行真实训练并生成非空 80 日 test 预测。

完成判定：真实 model 和 fresh HMM 可加载；test Top5 非空；baseline comparison 非空；无旧 HMM 输入、无未来泄漏、无 Windows 训练。

### Batch 3：M2 数据库推理与页面

1. 实现数据库批量 FeatureSource。
2. 实现 model bundle loader 和 Program 级推理。
3. 增加只读 API。
4. 增加页面影子视图和浏览器验收。

完成判定：目标多 Alpha Program 返回真实模型响应；单 Alpha Program 在没有匹配 bundle 时返回精确 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`，且不会误用多 Alpha 模型；两个 Program 的规则荐股均不受影响；Selection/Paper/模拟盘零写入。

Batch 1 完成后立即进入 Batch 2，不插入历史证据、归档或通用治理任务。

## 14. Design Acceptance Index

| ID | 设计要求 |
|---|---|
| F-301 | 精确绑定目标父包、manifest、combine run、Program 和 active binding，不做策略包二次准入 |
| F-302 | 精确读取两腿 38 个 seed pred、406 日逐日权重和权威 combined prediction，禁止 latest/扫描/减 seed |
| F-303 | 训练基础数据只来自现有 QE H5/Parquet/Qlib 日线 Bin 和 suspend sidecar，不读取生产数据库历史数据 |
| F-304 | 历史候选先截断正式 raw Top25，再在 Top25 内执行停牌过滤并形成 N<=20 的 selection_effective group；禁止从 rank26 以后补位 |
| F-305 | 离线与实时推理共享同一个 FeatureBuilder/schema，只允许 FeatureSource adapter 不同 |
| F-306 | 特征包含父包组件、量价、资金、估值、筹码、行业、市场和 fresh HMM，不静默删列或填充未知语义 |
| F-307 | HMM 使用当前文件数据从头拟合、真实 limit flag 和因果 forward-filter，旧 HMM 仅作外部对照 |
| F-308 | 5 日标签、可执行 entry/exit、成本、MFE/MAE、censor 和 relevance 公式完整冻结 |
| F-309 | 406 日按 256/5/60/5/80 时间切分，同日不跨 split，test 不参与选择 |
| F-310 | 使用真实 LightGBM LambdaRank，固定首模参数和 seed，不用规则/mock/随机冒充 |
| F-311 | 所有特征构建、HMM拟合和模型训练只在 WSL Conda执行，Windows不训练 |
| F-312 | 峰值内存低于8GB，批量/列投影/临时Parquet，目标小时级且不建SQLite或缓存平台 |
| F-313 | bundle 可加载并包含真实 model、fresh HMM、schema、request、split、metrics 和非空 test predictions |
| F-314 | API只读消费 Program/Selection/数据库当前输入，缺输入不得回放历史 PKL 或重跑策略包 |
| F-315 | 页面展示真实 EXPERIMENTAL_SHADOW Top5、原排名、模型排名、基线和错误状态 |
| F-316 | 多个 Program 独立；单/原生多 Alpha 共用输入合同，但 bundle 必须匹配 package/manifest/style，禁止跨包套用模型或恢复手工融合 |
| F-317 | 模型失败不阻断现有规则荐股，不用基线或旧模型冒充模型成功 |
| F-318 | Selection、Paper、模拟盘、QE和策略包业务逻辑零写入、零反向依赖 |
| F-319 | 不执行 Historical Range、Phase1R、SEALED/CAS、旧任务处理或历史证据工程 |
| F-320 | 不新增角色、审批、二次准入、收益门槛或未经确认的业务门禁 |
| F-321 | minute Bin不进入M0/M1/M2，首版价格范围后续只使用日线，盘中模型需用户另行确认 |
| F-322 | 所有失败使用 typed reason 和有效日志，不静默 fallback，不泄露敏感配置 |

## 15. Verification Plan

### 15.1 Batch 1

- 38/38 Prediction Store manifests 和 SHA 验证。
- combined prediction 1,802,507 行、406 日、schema/hash 验证。
- 406 个逐日权重和组合重算 parity。
- H5/Parquet/Bin/suspend schema 与日期覆盖。
- 每日 Top20 group 深度、过滤数和缺失原因。

### 15.2 Batch 2

- file/realtime raw snapshot 的 schema parity 单测。
- HMM train-only fit、causal inference 和旧产物排除测试。
- label maturity、entry/exit、涨跌停、停牌和 censor 边界测试。
- split 不重叠、purge=5、test immutable 测试。
- WSL identity、内存采样、训练失败 reason 和 bundle hash 测试。
- 真实 80 日 test predictions 与 5 组 baseline 报告。

### 15.3 Batch 3

- 目标多 Alpha Program 和一个单 Alpha Program API 行为测试。
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
| 个别 Prediction Store run 缺失或 hash 不符 | 明确列出 run_id 并停止该训练请求；不减 seed、不扫描 workspace、不换 latest |
| 合成重算与权威 combined prediction 不一致 | 报告最大误差和首个排序差异；修读取/归一化实现，不改变父包权威 |
| Top20 与正式 Selection 语义不一致 | 以当前 Program 的 HMM/risk/tradability 配置逐项核对；不引入 Historical Range 解决 |
| H5/Parquet 列缺失或全空 | 报告精确列和覆盖；若确属非必要特征，先修订 design/schema 版本，不运行时动态删列 |
| fresh HMM 个别行业覆盖不足 | 记录行业 unavailable；不得复用旧 HMM；整体 HMM bundle 非空且 test 有因果输出才完成 M1 |
| 训练内存或耗时超限 | 用阶段 RSS/wall-time 定位后改为更窄列投影或更小日期 shard；不建设新缓存平台 |
| 正式数据库字段暂不可用 | 只补目标 market 查询/adapter；模型通道显式 unavailable，规则荐股继续运行 |
| 模型 test 表现不佳 | 如实展示实验结果并进入 M5 特征/窗口迭代；不把结果门槛变成用户不可见门禁 |
| 模型错误影响现有模块 | 保持 Advisory-only import 和只读 API；protected-module 测试阻止反向依赖或写入 |
| 工作再次偏向历史平台 | F-319 直接判定 scope drift，停止该变更并回到 M0/M1/M2 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-301 | planned `backend/services/advisory_modeling/target_binding.py` | `backend/tests/advisory_modeling/test_model_first_target_binding.py` | design_ready | none |
| F-302 | planned `backend/services/advisory_modeling/qe_file_source.py` | `backend/tests/advisory_modeling/test_model_first_exact_prediction_roster.py` | design_ready | none |
| F-303 | planned `backend/services/advisory_modeling/qe_file_source.py` | `backend/tests/advisory_modeling/test_model_first_file_only_training.py` | design_ready | none |
| F-304 | planned `backend/services/advisory_modeling/candidate_group.py` | `backend/tests/advisory_modeling/test_model_first_candidate_stage.py` | design_ready | none |
| F-305 | planned `backend/services/advisory_modeling/shared_feature_builder.py` | `backend/tests/advisory_modeling/test_model_first_feature_source_parity.py` | design_ready | none |
| F-306 | planned `backend/services/advisory_modeling/feature_schema_v1.py` | `backend/tests/advisory_modeling/test_model_first_feature_missing_policy.py` | design_ready | none |
| F-307 | planned `backend/services/advisory_modeling/fresh_hmm.py` | `backend/tests/advisory_modeling/test_model_first_fresh_hmm.py` | design_ready | none |
| F-308 | planned `backend/services/advisory_modeling/labels.py` | `backend/tests/advisory_modeling/test_model_first_labels.py` | design_ready | none |
| F-309 | planned `backend/services/advisory_modeling/time_split.py` | `backend/tests/advisory_modeling/test_model_first_time_split.py` | design_ready | none |
| F-310 | planned `backend/services/advisory_modeling/reranker_training.py` | `backend/tests/advisory_modeling/test_model_first_real_lambdarank.py` | design_ready | none |
| F-311 | planned `scripts/advisory_model_train_wsl.py` | `backend/tests/advisory_modeling/test_model_first_wsl_only.py` | design_ready | none |
| F-312 | planned projected reader/trainer | `backend/tests/advisory_modeling/test_model_first_resource_budget.py` | design_ready | none |
| F-313 | planned `backend/services/advisory_modeling/model_bundle.py` | `backend/tests/advisory_modeling/test_model_first_bundle.py` | design_ready | none |
| F-314 | planned `backend/services/advisory_modeling/model_inference.py` + router | `backend/tests/advisory_modeling/test_model_first_api_input_authority.py` | design_ready | none |
| F-315 | planned Advisory page | `frontend/tests/advisory-model-first-shadow.spec.ts` | design_ready | none |
| F-316 | planned Program composition | `backend/tests/advisory_modeling/test_model_first_program_isolation.py` | design_ready | none |
| F-317 | planned inference error envelope | `backend/tests/advisory_modeling/test_model_first_baseline_continuity.py` | design_ready | none |
| F-318 | planned Advisory-only modules | `backend/tests/advisory_modeling/test_model_first_protected_module_isolation.py` | design_ready | none |
| F-319 | planned dependency boundary | `backend/tests/advisory_modeling/test_model_first_no_historical_dependency.py` | design_ready | none |
| F-320 | this design and code scan | `backend/tests/advisory_modeling/test_model_first_no_unapproved_gates.py` | design_ready | none |
| F-321 | planned input descriptor | `backend/tests/advisory_modeling/test_model_first_minute_exclusion.py` | design_ready | none |
| F-322 | planned typed errors/logging | `backend/tests/advisory_modeling/test_model_first_error_visibility.py` | design_ready | none |

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

本设计通过 F2 validator 和正式审核后即可进入 Batch 1/M0 代码开发，不再新增其它前置设计。Batch 1 的首个执行目标是生成一份真实 `FrozenAdvisoryTrainingRequestV1` 并完成 406 日 Top20/feature/label coverage；通过后立即进入 Batch 2 的真实 WSL 训练。
