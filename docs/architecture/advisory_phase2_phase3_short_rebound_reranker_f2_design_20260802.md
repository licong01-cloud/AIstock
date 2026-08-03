# AIstock Advisory Phase 2/3 SHORT_REBOUND 候选重排 F2 详细设计

> 日期：2026-08-02
> Feature tier：`F2`（Advisory 数据、WSL 训练、immutable bundle 与影子推理跨边界能力）
> 当前状态：`batch_a_contract_source_verified_batches_b_c_d_not_started`
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 上游数据合同：`docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
> 上游审计合同：`docs/architecture/advisory_phase0b_candidate_quality_modelability_f1_design_20260731.md`
> 业务边界：学术研究与历史分析；不产生订单、仓位、交易执行或实时投资建议

## 0. 文档定位与权威顺序

本文档合并落实父蓝图 Phase 2 的最小模型合同和 Phase 3 的首个真实模型垂直切片。首个目标只覆盖当前声明为 `SHORT_REBOUND` 的原生多 Alpha 父包，以冻结的权威 Top20 候选训练 Top20 到 Top5 的 LightGBM LambdaRank 研究影子模型。

冲突时按以下顺序处理：

1. 用户当前明确要求；
2. 父级模型蓝图的业务范围、隔离边界和阶段顺序；
3. Phase 0A 的 PIT/OOS/candidate/winner 身份；
4. Phase 1/1R 的 observation、label、source revision 与 SEALED snapshot 合同；
5. Phase 0B 的只读审计和统计口径；
6. 本文档的模型合同。

本文档不把策略包再次送入准入、资产完整性或可执行性审批。策略包进入系统时的准入结论保持权威；本阶段只校验本次模型操作实际消费的数据、版本、PIT、label、snapshot 和 bundle 身份。

## 1. Background / 背景

当前 15 日 Phase 0B 审计已证明读取、聚合和 exact retry 链路可用，但两个包的成熟收益标签覆盖均为 0，不能据此评价 HMM、候选深度或训练模型。首个真实功能必须转向多年训练输入和实际候选重排，不再处理旧 batch、旧 artifact root、历史归档或通用平台建设。

当前业务基线由 `selection_effective_rank` 决定。Phase 3 只能生成独立的 `advisory_model_rank/advisory_model_score` 研究影子证据，不能覆盖 Selection、Advisory 当前列表、Paper 或模拟盘的权威排序。

## 2. Scope / 范围

### 2.1 本阶段交付

1. 为首个目标原生多 Alpha 父包解析并冻结 `SHORT_REBOUND` style profile。
2. 冻结候选重排的 feature schema、label policy、ranking group、时间切分和实验注册表。
3. 从配置数据库读取 PIT 数据，构建一个覆盖“5 年拟合历史 + 20 日 fit/validation gap + 60 日 validation + 20 日 pre-test embargo + 300 日 test + 20 日 post-test label-maturity source tail”的新 `RETROSPECTIVE_RESEARCH_ONLY` SEALED base snapshot，并由同一 base snapshot 派生 2/3/5 年不可变训练视图。
4. 将训练数据一次性物化为 repo-external Parquet/JSON 文件；模型训练只在 WSL Conda 环境读取这些文件。
5. 在 WSL 训练固定的 LightGBM LambdaRank 组合，比较原始前5、HMM前5、模型前5、随机5和候选20等权。
6. 生成不可变研究 bundle、评估报告、exact retry receipt 和研究影子推理结果。
7. 以显式 capability/reason code 表达模型是否可用；没有合法 OOS 时保持 `MODEL_UNAVAILABLE`，不伪装为 `RERANK_READY`。

### 2.2 首个目标身份

首个目标是 Phase 0B 中已识别为 `SHORT_REBOUND` 的原生多 Alpha 父包 `pkg_ma_8ec5e389fa2c5e484a1ac7e9`。实现不得只按显示名称或当前最新包猜测目标；manifest 与组件身份以创建训练请求时显式冻结的权威值为准，训练请求必须携带并核对：

```text
package_id
package_manifest_sha256
package_asset_closure_hash
multi_alpha_parent_contract_version
selection_runtime_semantics_hash
effective_package_oos_cutoff
strategy_style_profile_id/hash
```

Program/binding/review 作为 lineage 保存，但不进入相同经济样本的重复加权。其它单 Alpha 或其它原生多 Alpha 包未被当前合同覆盖时返回 `MODEL_CONTRACT_NOT_AVAILABLE`，其现有荐股流程继续运行。

## 3. Non-goals / 非目标与边界

- 不建设通用 ModelOps、训练平台、自动调度、模型注册中心、champion/challenger 或自动晋级。
- 不设计角色、审批、人工确认、canary、enable gate、策略包重新批准或数据库备份门禁。
- 不训练 Phase 4 收益/持有期模型，不发布胜率、收益分位数或持股周期预测。
- 不训练 Phase 5 买入、止盈、止损或分钟路径模型。
- 不修改荐股页面；用户可见影子展示属于 Phase 6。
- 不修改 Selection、Paper、模拟盘、QE/RD-Agent、Qlib/backtest、QMT 或策略包推理业务逻辑。
- 不读取 QE 回测结果、回测 Parquet、Paper/模拟盘账户结果或人工买入记录。
- 不恢复、修改、迁移或归档旧 Phase 1R batch、旧 snapshot、旧 root 或 orphan build。
- 不新增数据库 schema；首个切片使用现有 SEALED snapshot 合同和 repo-external artifact。
- 不把全市场 universe outcome 或候选深池扩展作为首个重排模型的前置；本阶段固定消费权威 Top20。Recall/深池仍按父蓝图独立研究，不能由模型结果反推。

## 4. Design Acceptance Index / 设计验收索引

| design_item | 设计要求 |
|---|---|
| F-201 | 目标原生多 Alpha 父包解析到唯一 `SHORT_REBOUND` style profile，不做策略包二次准入 |
| F-202 | 数据只来自配置数据库和 Advisory immutable evidence，禁止回测/Paper/模拟盘污染 |
| F-203 | 新 SEALED base snapshot 覆盖 5 年拟合历史和 420 日 split/评估尾段，派生 2/3/5 年冻结视图，不复用 15 日审计数据训练 |
| F-204 | base snapshot 上方生成独立 `RerankerFeatureSnapshotV1`；feature/label/ranking group/PIT cutoff 全部版本化且可重建 |
| F-205 | 训练标签固定为 5 日扣费后风险感知 relevance，1/3/10/20 日只作诊断 |
| F-206 | 多 Alpha leg、五层 rank、HMM/risk 和市场/行业特征保留，HMM 不二次乘权 |
| F-207 | 时间切分冻结 fold 日期公式、label-as-of、rolling OOS、purge/embargo 和 refit 时点，禁止随机切分和未来数据 |
| F-208 | 2/3/5 年窗口、四个特征消融、三个诊断种子和唯一模型选择顺序在读取标签前预登记 |
| F-209 | 所有训练只在 WSL Conda 执行，Windows 仅准备文件和发起显式进程 |
| F-210 | immutable bundle 包含模型、schema、snapshot、split、环境、代码和评估完整闭合 |
| F-211 | 影子推理只产生 Advisory 派生副本，不覆盖 `selection_effective_rank` |
| F-212 | 原始前5、HMM前5、无 HMM 模型、含 HMM 模型、随机5和候选20完整对照 |
| F-213 | 数值能力阈值、最小 OOS 样本、regime、区间和多重检验在训练前冻结 |
| F-214 | retrospective bootstrap 永远不能发布 `RERANK_READY` 或用户可见模型排名 |
| F-215 | 缺失、版本冲突或训练失败以 typed reason 可见，不静默降级为模型成功 |
| F-216 | 模型通道失败不阻断现有荐股基线，也不影响其它 Program/包 |
| F-217 | shortlist Top5、候选 Top20 与 Program target_count 保持三个独立语义 |
| F-218 | deterministic LightGBM、稳定输入顺序和 exact retry 对同一请求生成相同 dataset/model/report semantic identity |
| F-219 | 训练与推理制品均位于显式 repo-external root，禁止猜测路径 |
| F-220 | 不新增角色、审批、策略包复核或未经确认的业务运行门禁 |
| F-221 | 首个实现不新增 DDL；Batch B 只使用明确列出的现有 Advisory DML 表，Batch A/C/D 无业务 DML |
| F-222 | 实现模块保持 Advisory 单向依赖，Selection/Paper/模拟盘/QE 无反向 import |
| F-223 | 设计、实现和验证不得交付子集、占位、mock-only 或静默 fallback |
| F-224 | 训练结果只回答相对排序，不冒充胜率、收益或价格预测 |

## 5. Architecture / 架构

### 5.1 离线训练流

```text
explicit SHORT_REBOUND training request
  -> admitted package identity reader (read-only, no re-admission)
  -> Phase 1/1R PIT observation + outcome label projection
  -> new RETROSPECTIVE_RESEARCH_ONLY SEALED base snapshot
     (5-year max fit history + 420-trading-day split/evaluation tail)
  -> Advisory-only RerankerFeatureSnapshotV1
     (feature rows + feature source revisions + formula registry)
  -> frozen 2y / 3y / 5y training-view manifests
  -> verified Parquet export under explicit repo-external root
  -> WSL Conda trainer
  -> registered experiment matrix and rolling OOS evaluation
  -> immutable AdvisoryCandidateRerankerBundle
  -> research-only shadow inference artifact
```

数据库提取只发生在 base snapshot 和派生特征快照构建阶段。训练阶段不连接数据库，只读取已经闭合的 Parquet/JSON 文件，避免数据库成为 epoch/模型组合的重复读取瓶颈。base snapshot 继续使用现有 Phase 1/1R writer；`RerankerFeatureSnapshotV1` 是 `advisory_modeling` 自有的文件制品，不向共享 snapshot schema 增加 role、字段或反向 import。

### 5.2 模块边界

计划新增独立模块：

```text
backend/services/advisory_modeling/
  contracts.py
  style_profile.py
  feature_schema.py
  feature_builder.py
  feature_snapshot.py
  market_regime.py
  label_policy.py
  training_view.py
  bundle_store.py
  shadow_inference.py

scripts/advisory_short_rebound_dataset.py
scripts/advisory_short_rebound_train_wsl.py
scripts/wsl/advisory_short_rebound_train.py
```

`advisory_modeling` 可以只读依赖 Phase 0A/1/1R 的正式合同和 verified Parquet reader。共享 Selection、simulation、Paper、strategy package、Phase 1 snapshot writer 和 QE 模块不得 import `advisory_modeling`。训练导出与影子推理必须调用同一个纯函数 `ShortReboundFeatureBuilderV1`；WSL trainer 不连接业务数据库，也不重新计算特征。

### 5.3 文件边界

不新增一组互相独立且容易分叉的 root 配置。离线命令只增加一个 repo-external `artifact_root`，其下按 content identity 确定性派生 `datasets/`、`training_exports/`、`model_bundles/` 和 `reports/`；repository root 使用当前明确 checkout。数据库和 WSL 连接只复用现有 `.env` 配置，不增加第二套 DSN 或 Conda 变量。

执行输入为：

```text
artifact_root
repository_root
existing database prefix from .env
QLIB_WSL_DISTRO
QLIB_WSL_CONDA_SH
QLIB_WSL_CONDA_ENV
```

`artifact_root` 必须真实存在且位于 repo-external containment 内；其子目录由程序创建但不得越界。snapshot 文件位置由数据库 catalog 中已冻结的 content URI 和既有 dataset store 配置解析，不要求用户重复填写。缺失配置时返回错误，不扫描“最新”目录、不回退默认根、不猜测 `.env` 以外的连接。

## 6. Contracts / 核心契约

### 6.1 StrategyStyleProfileV1

```text
schema_version = advisory_strategy_style_profile_v1
style_family = SHORT_REBOUND
primary_horizon_trading_days = 5
supported_horizons = [1, 3, 5, 10, 20]
signal_decay_prior = FAST
label_objective = RISK_AWARE_NET_RETURN_RANKING
candidate_observation_top_k = 20
shortlist_top_n = 5
profile_version
package_id / package_manifest_sha256 / package_asset_closure_hash
selection_runtime_semantics_hash
effective_package_oos_cutoff
profile_payload_sha256
```

profile 是 Advisory 模型配置，不修改策略包 manifest。profile 与目标包身份不匹配时只关闭模型通道。

### 6.2 DatasetBuildRequestV1

一个请求覆盖最大 5 年训练历史和独立评估尾段，并冻结三个 view：

```text
request_schema_version
style_profile_id/hash
package/manifest/runtime identity
multi_alpha_component_identity_set_hash
decision_date_start / decision_date_end
requested_windows_years = [2, 3, 5]
evaluation_tail_trade_days = 420
candidate_observation_top_k = 20
feature_schema_id/hash
feature_formula_registry_hash
feature_query_registry_hash
market_regime_policy_template_id/hash
label_policy_id/hash
source_revision_set_id/hash
universe_policy_set_id/hash
calendar_version/hash
evidence_scope = RETROSPECTIVE_RESEARCH_ONLY
repository_commit
final_fit_as_of
request_semantic_hash
```

同一个 SEALED base snapshot 是候选、stage、label 和 source revision 的物理权威；它至少覆盖最长 5 年拟合窗口，以及其后的 20 日 fit/validation gap、60 日 validation、20 日 pre-test embargo、300 日 test 和 20 日 post-test label-maturity source tail，共 420 个交易日。最后 20 日只提供 test outcome 成熟所需行情/企业行动/source evidence，不进入 decision group、fit、validation 或 test。2/3/5 年 view manifest 只保存各自 fit/validation/test date boundary、group/member hashes、base snapshot ref/hash 和 feature snapshot ref/hash，不复制三份相同文件。若数据库可复算日期不足，构建仍产出准确 coverage receipt，但不得把不足窗口标记为可训练。首模只要求候选 observation/outcome 闭合；`universe_outcomes` 不作为训练前置，也不得用候选集合伪造 universe denominator。

### 6.3 FeatureSchemaV1

每个特征带 `name/dtype/unit/availability_cutoff/source_role/query_template_id/formula_id/formula_version/missing_policy`。`FeatureFormulaRegistryV1` 保存完整 canonical payload，不允许实现只记录名称或 hash。第一版冻结以下特征族：

1. **候选阶段**：`alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、`selection_effective` 的 rank/score、候选内 percentile 和相邻分差。
2. **多 Alpha**：逐 leg score/rank/weight、leg 数、加权均值、离散度、方向一致度、最大 leg 支配度及各 leg model/asset hash。
3. **个股 PIT**：1/3/5/10/20/60 日收益、距 20/60 日高低点、波动率、振幅、成交额、换手、量价变化和资金流。
4. **行业 PIT**：行业相对强度、宽度、成交/资金扩散、行业映射 hash。
5. **HMM**：enabled、snapshot/preset、coefficient、as-of/effective date、freshness、artifact content hash。
6. **risk policy**：enabled、policy hash、can_buy、multiplier/delta/penalty 及调整前后差异。
7. **市场上下文**：PIT universe 等权收益/横截面波动、市场宽度和涨跌停分布；regime 只在 split 后由各 fold fit statistics 派生，不写入共享 feature row。
8. **候选组上下文**：候选数、行业集中度、score dispersion 和相关簇统计。

特征只使用 `decision_as_of_trade_date` cutoff 前已知数据。T+1 开盘、停牌、涨跌停和未来路径只能进入 label。LightGBM 可识别的缺失值必须同时有 missing indicator；required identity/rank/source 特征缺失时该日期模型推理返回 `MODEL_FEATURE_CLOSURE_INCOMPLETE`，不能只移除缺失股票再补满 Top5。

第一版公式口径冻结为：

| formula_id | 公式与输入 | PIT/source 约束 |
|---|---|---|
| `candidate_rank_percentile_v1` | `n>1 ? (n-rank)/(n-1) : 0.5`；四个 stage 分别计算 | 只读 base snapshot 同日 stage candidates |
| `candidate_score_gap_v1` | 当前 score 与相邻上一/下一 rank score 的差；边界为空并带 missing flag | score direction 与 tie policy 取自同日 stage summary |
| `multi_alpha_consensus_v1` | 对冻结 leg score 做 weighted mean、weighted std、sign agreement、`max(abs(weight*score))/sum(abs(weight*score))` | leg/weight/model identity 必须来自 base snapshot `component_evidence_json`，不得查询 current package |
| `adjusted_return_v1` | `adj_close_T/adj_close_{T-h}-1`，`h in {1,3,5,10,20,60}` | close/adj_factor revision 的 `available_at<=decision_cutoff_ts` |
| `realized_volatility_v1` | 最近 h 日 `log(adj_close_t/adj_close_{t-1})` 的 sample std，`ddof=1` | 至少 `h` 个合法交易日，否则缺失 |
| `distance_to_extreme_v1` | `adj_close_T/max(adj_high_{T-h+1:T})-1` 与 `adj_close_T/min(adj_low_{T-h+1:T})-1` | 企业行动一致的 high/low/adj_factor |
| `liquidity_state_v1` | amount/turnover 的 5/20 日均值、当前值除以 20 日均值、20 日 log-amount z-score | 停牌日保留零成交事实，不向前填充 |
| `moneyflow_state_v1` | 主力净流入除以成交额、5/20 日和及方向一致率 | 分母为 0 时值缺失并置 flag，不填 0 |
| `industry_context_v1` | PIT 行业成员等权 5/20 日收益、`close>MA20` 比例、正净流入成员比例 | 使用 T 日可用行业映射和成员集合 hash |
| `market_context_v1` | PIT universe 个股 5/20 日收益的等权均值/横截面波动、`close>MA20` 比例、涨跌停比例 | universe、价格限制政策和有效成员集合均进入 hash；不新增指数数据源 |
| `candidate_group_context_v1` | 候选数、行业 Herfindahl、四阶段 score std、leg disagreement 均值 | 仅使用同日 base candidates |

`h` 个交易日窗口按冻结 calendar 展开，不按自然日。任何滚动统计的均值、标准差、分母、空值和最少观测数均由 formula payload 固定，不能由实现选择 pandas 默认值。

`adj_open/high/low/close = raw_li * adj_factor`，收益和距离特征均为无量纲小数；金额/资金流在计算比率前按现有 price-unit 合同转换到同一单位。不得混用 raw li、yuan 和未复权路径。

#### 6.3.1 RerankerFeatureSnapshotV1

派生特征快照是 `advisory_modeling` 独占的 repo-external content-addressed artifact，不进入 `SNAPSHOT_ARROW_SCHEMAS_V1`，也不修改 Phase 1 writer。目录固定包含：

```text
feature_snapshot_manifest.json
feature_schema.json
feature_formula_registry.json
feature_source_revisions.parquet
feature_rows/date=YYYY-MM-DD/part-00000.parquet
completion_receipt.json
```

每个 feature row 的稳定键为 `(base_snapshot_id, canonical_signal_id, observation_version_id, symbol)`，并保存 `decision_cutoff_ts`、base candidate/stage hashes、feature payload hash、formula registry hash、query registry hash、feature source revision set hash 和 builder code closure hash。manifest 覆盖全部分区 descriptor；receipt 最后原子发布。相同请求必须 exact readback，禁止覆盖。

市场/行业/行情特征由 `ShortReboundFeatureBuilderV1` 使用冻结 query registry 从 `.env` 数据库读取；HMM、risk、四阶段排名和多 Alpha leg 只消费 base snapshot 的 exact stage/component evidence，禁止事后查询 latest HMM、latest policy 或 current package。影子推理调用同一 builder 和 formula registry；训练导出不得另写第二套计算。

#### 6.3.2 FrozenFeatureQueryRegistryV1

第一版不增加共享 catalog query。`FrozenFeatureQueryRegistryV1` 在 `advisory_modeling` 内冻结下列现有只读模板的 SQL bytes、参数 schema、结果 schema、template/version/hash 和来源 repository commit；运行时不 import 或修改共享 catalog。每个 bound parameter hash、partition hash、row count 和 availability event 写入 `feature_source_revisions.parquet`：

```text
historical_pit_universe_existing_readonly
historical_trading_calendar_window
historical_market_history_window
historical_decision_mark_daily_market
historical_decision_mark_market_state
historical_fundamental_moneyflow_window
historical_suspend_lookup
historical_industry_membership
```

同一日期/窗口/参数的 source partition 只读取一次并在本次 feature build 的临时 spool 中复用，不能按候选或模型组合重复查询数据库。任一模板缺失、schema hash 改变或 source revision 不闭合时整个 feature snapshot 失败；不得跳过该特征族后继续发布同一 schema id。

### 6.4 RankingLabelPolicyV1

主标签固定为 5 个交易日风险感知 relevance：

```text
return_5 = net_excess_return_5
mfe_5 = max(0, executable_mfe_5)
mae_loss_5 = max(0, -executable_mae_5)
raw_utility_5 = return_5 + 0.25 * mfe_5 - 0.50 * mae_loss_5
```

三个输入均为同一 label policy 下的无量纲小数收益。`0.25/-0.50` 表示对有利路径只给有限奖励、对同幅不利路径施加两倍惩罚；该权重随 label policy v1 冻结，不进入 12 个候选搜索，也不允许根据本轮结果调参。权重敏感性只能作为另一个明确版本的后续研究，不能覆盖 v1 报告。

在同一 ranking group 内先对不同 `raw_utility_5` 值做 ascending dense rank。设不同值数量为 `m`、当前 dense rank 从 0 开始为 `d`，`m>1` 时 `relevance=floor(4*d/(m-1))`；`m=1` 时全部 relevance 为 0 且该 group 标记 `NO_LABEL_VARIATION`，不进入拟合。相同 utility 必须获得相同 relevance；symbol 只用于序列化顺序，`selection_effective_rank` 绝不进入标签或 tie-break。`label_gain=[0,1,3,7,15]`。该 relevance 仅是排序训练目标，不是收益预测。

`return_5` 精确消费候选 owner、`projection=RETURN_NET_EXCESS`、`horizon=5`、`maturity=MATURED` 的 projection value；MFE/MAE 同样消费候选 owner 和同一 label policy 的 `EXECUTABLE_MFE/MAE`。5 日绝对净收益、1/3/10/20 日净收益、PATH_MFE/MAE、positive-return 和 signal decay 只作诊断/分桶，不参与首个主标签择优。label policy 必须复用 Phase 1 的 entry、成本、benchmark、企业行动、terminal/censor 和可交易性口径；不得另写简化收益计算。

ranking group 固定为：

```text
(decision_as_of_trade_date,
 target_trade_date,
 canonical_signal_scope_hash,
 label_policy_hash)
```

少于 2 个有成熟主标签候选的 group 标记 `GROUP_NOT_MODELABLE`，保留 coverage 证据但不进入 LambdaRank 拟合。2 至 4 个候选的 group 可以拟合和推理；`Precision@5` 的未占槽位按失败计入固定分母 5，shortlist 显示真实 `N/5`，不得补入其它日期或其它 stage 股票。每个 fold 的 eligible decision dates、modelable dates、`NO_LABEL_VARIATION` dates 和候选深度分布必须完整报告。

### 6.5 时间切分与 OOS 合同

- 禁止随机拆分股票行。
- 将 snapshot 最后的 300 个 eligible decision dates 按升序切成五个连续且不重叠的 60 日 outer test blocks；不足 300 日时只能生成 coverage report。
- 对每个 test block `j`，`test_start_j/test_end_j` 取其首末日期；`pre_test_embargo_j` 是 `test_start_j` 前连续 20 个交易日；`validation_j` 是 embargo 前连续 60 个交易日；`fit_validation_gap_j` 是 validation 前连续 20 个交易日。
- `fit_end_j` 是 `fit_validation_gap_j` 前一交易日；窗口 `y in {2,3,5}` 的 `fit_start_{j,y}` 是 `fit_end_j` 向前 y 个完整日历年后遇到的第一个交易日。拟合数据只含 `[fit_start_{j,y},fit_end_j]`，validation 和两个 gap 均不进入拟合。
- `fold_training_as_of_j` 固定为 `test_start_j` 前一交易日收盘 cutoff。fit/validation 行只有在主标签及其所有 source revision 的 `available_at/computed_at <= fold_training_as_of_j` 时可用；不得依据后来补算结果回写既有 fold。
- 对 `j>0`，早期 test block 只有在其标签于当前 `fold_training_as_of_j` 前已经成熟时，才可能自然落入新的 fit/validation 日期范围；程序不得用“曾经是 test”永久排除，也不得在成熟前提前使用。
- 每个候选行按决策日归属唯一 split；同一日所有股票必须位于同一 split。
- `effective_package_oos_cutoff + embargo` 之后且 package/runtime/HMM vintage 当时已存在的 test 才能计为 formal OOS；其余明确为 retrospective research。

`SplitPlanV1` 必须逐 fold 保存上述全部日期集合、calendar positions、label-as-of、可用 observation/label/member hashes 和排除原因；任何边界变化都会产生新 split hash，不允许 trainer 自行推导另一套日期。

### 6.6 预登记实验矩阵

模型族固定为 LightGBM `lambdarank`，主指标 `ndcg@5`。训练窗口 `{2,3,5}` 与特征组合形成 12 个预登记候选：

```text
CORE
CORE_PLUS_HMM
CORE_PLUS_RISK
CORE_PLUS_HMM_PLUS_RISK
```

固定训练参数：`num_leaves=31`、`learning_rate=0.03`、`n_estimators=600`、`min_data_in_leaf=80`、`feature_fraction=0.8`、`bagging_fraction=0.8`、`bagging_freq=1`、`lambda_l1=0.1`、`lambda_l2=1.0`、`early_stopping_rounds=80`、`deterministic=true`、`force_col_wise=true`、`num_threads=1`。主 seed 为 `20260710`，并显式设置 `seed/feature_fraction_seed/bagging_seed/data_random_seed`；`20260711/20260712` 只作稳定性诊断。参数、软件版本、UTF-8/symbol/date/group 稳定输入顺序和矩阵 hash 在读取 outcome label 前冻结；本阶段不做自动超参搜索。

HMM/risk 组合是消融，不是再次乘权。若 `hmm_enabled=false`，HMM 组合不适用于该 group并显式记录，不用零值冒充 HMM 证据。

#### 6.6.1 MarketRegimePolicyV1

regime 只用于分层报告和 capability coverage，不作为人工门禁。每个 fold 用 fit 数据冻结均值/标准差：

```text
trend_z = 0.5 * z(pit_universe_equal_weight_return_20) + 0.5 * z(market_breadth_above_ma20)
BEAR     if trend_z <= -0.5
BULL     if trend_z >=  0.5
NEUTRAL  otherwise
```

request 冻结不含样本统计量的 `MarketRegimePolicyTemplateV1`。z-score 统计量只由该 fold fit rows 计算；每个 fold 生成独立 `FittedMarketRegimePolicyV1`，保存 universe hash、feature formula ids、thresholds、fit statistics、calendar、decision cutoff 和 `fitted_market_regime_policy_hash`，并进入 SplitPlan/bundle。validation/test 只应用本 fold 的冻结统计量，feature snapshot 不保存 regime label。缺少任一输入时该日期 regime 为 `UNAVAILABLE`，不得按 NEUTRAL 处理；它仍保留在整体指标中，但不计入三 regime 覆盖数量。final refit 同样生成一个独立 final fitted policy，仅用于后续 shadow 分层解释。

#### 6.6.2 唯一模型选择与最终 refit

每个“窗口 × 特征组合”是一个候选。研究 bundle 选择与 `RERANK_READY` 是两个不同状态：

- `RESEARCH_CANDIDATE_ELIGIBLE` 只要求五个 temporal holdout folds 完整、modelable coverage 不低于 95%、PIT/feature/label/bundle/isolation 无错误以及三个 seed 全部产生诊断结果；它不要求 formal OOS，也不要求通过 §7.2 的收益阈值。
- 在全部 research-eligible 候选中，使用主 seed 的 date-level 指标按下列 immutable lexicographic key 选择唯一配置；两个诊断 seed 只进入稳定性报告，不参与候选排名。
- 选出的研究配置始终可以形成 `RESEARCH_BUNDLE_COMPLETE`；只有它另外满足 §7.2 的 formal OOS 和全部数值标准时，capability 才能成为 `RERANK_READY`。否则 bundle 明确保存 `MODEL_UNAVAILABLE` 及未达条款。

唯一选择顺序：

```text
1. NDCG@5 uplift 95% lower bound descending
2. mean five-day net-excess-return uplift descending
3. EXECUTABLE_MAE loss ratio ascending
4. turnover uplift ascending
5. feature set order: CORE < CORE_PLUS_HMM < CORE_PLUS_RISK < CORE_PLUS_HMM_PLUS_RISK
6. training window years ascending
7. candidate experiment id ascending
```

所有用于选择的数值先量化为 12 位小数再比较，最终 experiment id 保证唯一；不得使用未预登记指标或人工偏好改选。唯一配置确定后，取其五个主-seed fold `best_iteration` 的升序中位数作为 `final_n_estimators`，范围固定在 `[1,600]`；final refit 不再 early-stop。它以 request 中显式冻结的 `final_fit_as_of` 为 label/source availability cutoff，选择此前最后一个主标签已成熟的 decision date 作为 `final_fit_end`，按相同窗口/feature/schema/label 和主 seed 做一次 final refit；不得扫描运行时“最新”日期。fold models、每个 best iteration、选择证据和 final model 均进入同一 bundle。OOS 指标属于 fold models，不能描述成 final refit 自身的新 OOS 结果。

### 6.7 WSLTrainingRequestV1

Windows 侧只执行：校验显式路径、生成请求、把路径转换为 WSL 路径、调用指定 distro/Conda。真正的 Parquet 读取、训练、评估和 bundle 生成进程必须报告：

```text
runtime_os=linux
runtime_kernel contains microsoft/WSL
conda_environment_name
python/lightgbm/pyarrow/numpy/pandas versions
environment_lock_hash
training_code_commit/closure_hash
```

不满足 WSL 身份时返回 `MODEL_TRAINING_REQUIRES_WSL`。不得在 Windows Python 继续训练。

### 6.8 ImmutableModelBundleV1

bundle 目录必须 content-addressed、写临时目录后原子发布，至少包含：

```text
bundle_manifest.json
models/final_model.txt
models/selected_folds/fold-*/model.txt
style_profile.json
feature_schema.json
feature_formula_registry.json
feature_snapshot_ref.json
market_regime_policy_template.json
fitted_market_regimes/fold-*.json
fitted_market_regimes/final.json
label_policy.json
dataset_snapshot_ref.json
training_views.json
split_plan.json
experiment_registry.json
training_config.json
environment_lock.json
oos_metrics.json
baseline_comparison.json
feature_importance.json
model_selection_receipt.json
model_card.md
completion_receipt.json
```

`bundle_semantic_hash` 覆盖除生成时间和物理 URI 外的全部语义内容以及每个模型文件的 SHA-256；`bundle_id=advrerank_<hash prefix>`。completion receipt 最后发布，是唯一 COMPLETE 标志。相同请求必须在冻结 WSL/environment/input ordering/LightGBM deterministic 参数下生成逐文件相同的 bundle；不一致返回 `MODEL_EXACT_RETRY_CONFLICT`，禁止覆盖已有制品。

### 6.9 ShadowInferenceResultV1

输入是单个冻结候选组和 exact bundle ref/hash。输出包括每只候选的 baseline rank、normalized model score、model rank、Top5 membership、feature closure hash、bundle hash 和 reason codes。

模型 score 在组内按稳定 percentile 归一为 `[0,1]`；完全相同的 model score 只按 symbol 升序确定 model rank，禁止用任一 baseline rank 打破平局。结果写入隔离 research artifact，不写 Selection artifact、不修改现有 candidate、list 或 Program。模型不可用时输出 `capability_status=MODEL_UNAVAILABLE` 和原因；基线业务仍按原逻辑运行，但不得把基线 rank 填入 `advisory_model_rank` 冒充模型结果。

## 7. 能力判定与数值标准

### 7.1 Research bundle 完整性

research bundle 必须满足：

- 三个窗口均有明确 coverage；可训练窗口至少 480/720/1200 个有效决策日，分别对应 2/3/5 年目标；
- 每个 fold 都报告 eligible/modelable/no-label-variation/feature-missing dates 和候选深度；少于 2 个候选不伪造 ranking group；
- snapshot、view、feature、label、split、environment、code 和 model hash 全部闭合；
- WSL 三个固定种子均成功，失败种子不能被静默丢弃；
- exact retry receipt 一致。

不足时仍可发布 typed dataset/training coverage report，但不能发布 COMPLETE model bundle。

### 7.2 `RERANK_READY(SHORT_REBOUND)` 自动证据标准

以下标准是模型 capability 的程序化分类，不是人工审批，也不阻断荐股页面或现有列表：

以下 8 项只评价 §6.6.2 已确定的唯一 research bundle 配置；它们不参与重新选择另一个候选：

1. formal OOS 至少 300 个独立 eligible decision dates，feature/label 闭合且候选数至少 2 的 modelable coverage 不低于 95%；至少 3 个冻结 market regime 各有 40 个 modelable decision dates，任一 regime 不超过 modelable 日期的 70%。该值高于 Phase 0A 的 252 日推断性最低样本，不按股票行数替代日期数。
2. 相对 `selection_effective_rank` Top5，paired date-level `NDCG@5` 平均提升至少 `0.02`，95% block-bootstrap 下界大于 0。
3. `Precision@5` 使用 Phase 0A `SHORT_REBOUND` 冻结 winner `RETURN_NET_EXCESS,h=5,GT,0`，固定分母为 5；平均提升至少 `0.03`，95% block-bootstrap 下界大于 0。
4. Top5 五日扣费后超额收益平均提升至少 `0.001`（10 bps），95% block-bootstrap 下界大于 0；绝对净收益同时报告但不是本项替代指标。
5. Top5 `EXECUTABLE_MAE_5` 损失幅度不超过 baseline 的 `1.05` 倍；换手不超过 baseline 加 10 个百分点。
6. 三个固定种子的逐日 Top5 Jaccard 中位数至少 `0.60`；主 seed 的相同 bundle exact inference 必须完全一致，两个诊断 seed 不能参与事后选优。
7. 12 个预登记候选使用主 seed 的逐日结果，以 5,000 次 deterministic stationary block bootstrap SPA 控制 data-snooping，主收益比较 `p<=0.10`；次级指标采用 Benjamini-Yekutieli `q<=0.10`。两个诊断 seed 只验证稳定性，不参与 SPA 输入或模型选优；bootstrap seed/block rule 复用 Phase 0A 冻结口径。
8. 不得存在 PIT、vintage、label、source revision、bundle 或 isolation finding。

即使数值满足，纯 `RETROSPECTIVE_RESEARCH_ONLY` 数据仍只能得到 `RESEARCH_BUNDLE_COMPLETE + MODEL_UNAVAILABLE(NO_FORMAL_OOS)`。只有合法 formal OOS 满足全部条款时才可产生 `RERANK_READY` 证据；Phase 6 是否展示由后续设计决定。

### 7.3 对照定义

所有对照使用相同日期、相同候选、相同 label/cost/benchmark：

- `RAW_TOP5`：`alpha_raw_rank` 前5；
- `HMM_TOP5`：`hmm_adjusted_rank` 前5，仅在 HMM 证据完整时计算；
- `SELECTION_TOP5`：`selection_effective_rank` 前5，主 baseline；
- `MODEL_TOP5_NO_HMM`；
- `MODEL_TOP5_WITH_HMM`；
- `RANDOM5`：由 request hash、decision date、symbol 确定性采样；
- `CANDIDATE20_EQUAL_WEIGHT`：全部权威候选等权。

不得跨 package 合并为总榜，也不得按股票行数加权日期。

## 8. Typed Reasons / 错误可见性

首批 reason code：

```text
MODEL_CONTRACT_NOT_AVAILABLE
MODEL_STYLE_PROFILE_MISMATCH
MODEL_DATASET_SNAPSHOT_NOT_SEALED
MODEL_DATASET_WINDOW_INSUFFICIENT
MODEL_FEATURE_CLOSURE_INCOMPLETE
MODEL_FEATURE_SNAPSHOT_INCOMPLETE
MODEL_FEATURE_QUERY_REGISTRY_MISMATCH
MODEL_LABEL_CLOSURE_INCOMPLETE
MODEL_PIT_VINTAGE_CONFLICT
MODEL_MARKET_REGIME_UNAVAILABLE
MODEL_SPLIT_PLAN_MISMATCH
MODEL_TRAINING_REQUIRES_WSL
MODEL_TRAINING_ENVIRONMENT_MISMATCH
MODEL_EXPERIMENT_REGISTRY_MISMATCH
MODEL_SELECTION_NOT_UNIQUE
MODEL_BUNDLE_INCOMPLETE
MODEL_BUNDLE_HASH_MISMATCH
MODEL_EXACT_RETRY_CONFLICT
MODEL_NO_FORMAL_OOS
MODEL_OOS_SAMPLE_INSUFFICIENT
MODEL_OOS_THRESHOLD_NOT_MET
MODEL_INFERENCE_INPUT_MISMATCH
MODEL_SHADOW_WRITE_FAILED
```

错误日志记录 operation/request/bundle/snapshot 的非敏感 identity、阶段和 reason，不输出 DSN、密码或整份特征行。任何异常不得转换为空成功报告、零收益或伪 Top5。

## 9. Implementation Plan / 实施方案

### Batch A：最小合同与静态闭合

1. 实现 style、feature、label、view、experiment、bundle 和 reason contracts。
2. 实现 canonical hash、严格 readback 和显式 root containment。
3. 添加 isolation/ownership/test-plan catalog。

### Batch B：多年 snapshot 与训练文件

1. 复用现有 Historical Range、Phase 1/1R writer 和 verified reader，不修改共享 schema、writer 或其它消费者语义。
2. 从配置数据库按冻结请求生成覆盖 5 年拟合历史和 420 个交易日 split/评估尾段的新 SEALED base snapshot。
3. 用 `ShortReboundFeatureBuilderV1` 生成独立 `RerankerFeatureSnapshotV1`，逐分区读回验证 DB/base/feature identity。
4. 发布 2/3/5 年 view manifest 和 WSL 可读训练文件。
5. 运行 Phase 0B 数据质量审计，确认成熟标签、候选覆盖和 feature closure；不足准确停止在数据报告。

Batch B 的生产 DML 不是待开发者选择的未知范围。若使用现有完整链路，只允许正式 repositories 对以下既有 Advisory 表执行其已有幂等写入；禁止手写旁路 SQL、修改 Selection/Paper/模拟盘表或新增表列：

```text
Historical Range:
  app.advisory_historical_range_request_key
  app.advisory_historical_range_batch
  app.advisory_historical_range_run
  app.advisory_historical_range_day_run
  app.advisory_historical_range_day_attempt
  app.advisory_historical_range_candidate
  app.advisory_historical_range_list_version
  app.advisory_historical_range_list_item
  app.advisory_historical_range_episode_snapshot
  app.advisory_historical_range_operation
  app.advisory_historical_range_operation_attempt
  app.advisory_historical_range_outcome
  app.advisory_historical_range_summary

Phase 1 observation/label/source:
  app.advisory_source_revision_set
  app.advisory_source_revision_member
  app.advisory_capture_batch
  app.advisory_capture_plan
  app.advisory_capture_batch_evidence_membership
  app.advisory_signal_observation
  app.advisory_signal_observation_version
  app.advisory_signal_observation_lineage_identity
  app.advisory_signal_observation_lineage_payload
  app.advisory_signal_stage_evidence
  app.advisory_signal_stage_candidate_identity
  app.advisory_signal_stage_candidate_payload
  app.advisory_outcome_label
  app.advisory_outcome_label_payload

Phase 1 dataset/snapshot:
  app.advisory_dataset_build
  app.advisory_dataset_build_attempt
  app.advisory_dataset_build_event
  app.advisory_dataset_attempt_file
  app.advisory_dataset_blob
  app.advisory_dataset_snapshot
  app.advisory_dataset_snapshot_file
  app.advisory_dataset_snapshot_observation
  app.advisory_dataset_snapshot_label
  app.advisory_dataset_snapshot_blob_ref
```

`RerankerFeatureSnapshotV1`、training views、model bundle 和 report 只写 `artifact_root`，不写数据库。已有表/约束不足时必须返回设计偏差并停止代码阶段，不能临场新增 DDL。

### Batch C：WSL 训练与评估

1. 实现 WSL launcher 与 Linux trainer。
2. 按冻结 SplitPlan 运行 12 个组合、主 seed、两个诊断 seed 和 rolling OOS。
3. 生成基线、消融、多重检验，按唯一 lexicographic key 选择配置并完成 final refit。
4. 发布 immutable research bundle、model-selection receipt 与 exact retry receipt。

### Batch D：研究影子推理

1. 对冻结历史候选运行 shadow inference。
2. 验证五层 rank、Top5、理由和 bundle lineage。
3. 验证关闭/错误时现有荐股、Selection、Paper、模拟盘完全不变。

每个 Batch 独立提交和审核；不得用后续 Batch 的 mock 替代前一 Batch 的正式合同。

## 10. Verification Plan / 验证方案

### 10.1 L0/L1

- contract validation、canonical hash、equal-utility relevance、ranking group、fold date formula、label-as-of、split/purge/embargo、score normalization；
- feature formula registry、base/feature row closure、training/inference builder parity、market regime fit-only normalization；
- deterministic LightGBM、stable input ordering、唯一模型选择、root containment、WSL identity、bundle atomic publish/readback/exact retry；
- typed reason 与无静默 fallback；
- import boundary、共享 Phase 1 writer 零修改和 package/no-re-admission oracle。

计划测试：`backend/tests/advisory_modeling/`，并通过 `nox -s advisory_modeling_backend` 运行变更模块和真实直接依赖。

### 10.2 L2 真实数据/训练

- 在 DEV 合同环境验证完整 Historical Range/Phase 1 allowlist DML、回滚和 artifact 写入；随后从 `.env` 配置的生产数据源读取行情。生产只读不设置额外审批；生产 DML 只使用 §9 Batch B 列出的既有 Advisory repositories/tables。
- DB/base snapshot/feature snapshot 按日期、symbol、feature/label/source/formula/query hash 抽样一致。
- WSL 真实 LightGBM 训练、bundle 加载和历史 shadow inference。
- 2/3/5 年窗口、五个 fold、12 个组合、三个种子、coverage、regime、唯一选择、final refit、全部对照和 exact retry 无缺项。

### 10.3 L3/L4/L5

- 本阶段无 API/UI，因此 L3/L4 为 `noop`。
- 长窗口 rolling OOS 和广泛 regime 验证交由 Validation Center/nightly，消费 compact receipt。
- 受影响模块以外的 Selection/Paper/模拟盘/QE 只执行 import-boundary 和直接业务 oracle，不运行无关大套件。

### 10.4 DESIGN-COMPLIANCE-001 复核

合入前逐项证明：

1. 全部 feature/label/window/experiment/bundle/shadow 合同均有实现和测试，不把单窗口、单 seed 或静态 JSON 当作完整交付。
2. 缺失、冲突、训练失败和 capability 不足均有 typed reason/log/receipt，无默认值、空成功或 baseline 冒充模型输出。
3. rank、PIT、OOS、label、Top5/Top20、Program 和 package 语义与父蓝图一致。
4. 没有角色、审批、package re-approval、运行时 DDL 或未经确认的业务门禁。

## 11. Risks / 失败模式与处置

| 风险 | 后果 | 处置 |
|---|---|---|
| 当前包研发期数据被冒充 OOS | 虚假模型 ready | 永久保留 retrospective scope；按 effective cutoff+embargo 分类 |
| 多年回放使用后来数据 | lookahead | feature availability cutoff/source revision 逐字段闭合 |
| train/inference 使用不同特征实现 | train-serving skew | 唯一 FeatureBuilder、formula/query registry 和逐行 closure hash |
| 相同 outcome 按 baseline rank 拆标签 | 模型被迫复制基线 | equal utility equal relevance；baseline rank 禁止进入标签 |
| fold 边界或补算标签漂移 | 日期泄漏 | SplitPlan 保存全部日期与 label-as-of；20 日双 gap |
| 只挑最佳窗口/消融 | data snooping | 12 个组合预登记、SPA/BY、唯一 lexicographic 选择 |
| LightGBM 并行非确定性 | exact retry hash 冲突 | deterministic/force_col_wise/单线程/全部 seed/稳定输入顺序 |
| HMM 再次机械乘权 | 双重调整 | HMM 仅作为特征和消融 |
| 少于 5 只后补满 Top5 | 排名语义漂移 | 真实 N/5；空槽 Precision 失败；modelable coverage 至少 95% |
| 训练频繁查数据库 | 数据库瓶颈 | 一次 SEALED 文件物化，WSL 只读文件 |
| bundle 部分写入 | 不可复现 | 临时目录、全量 hash、receipt-last 原子发布 |
| 模型 import 进入共享链 | 阻碍选股/模拟盘 | 独立 Advisory 模块和反向 import 测试 |
| Batch B 临场扩大 DML/DDL | 共享链路受影响 | 固定既有表 allowlist；新特征只写 artifact root；DDL=noop |
| 模型结果被当作收益概率 | 用户误解 | Phase 3 只输出相对 rank/score，禁止概率字段 |
| 为首模搭建通用平台 | 延迟真实功能 | 固定单风格、单模型族、无 scheduler/registry/UI |

## 12. Rollout / 发布与回滚

本阶段发布顺序是 source contract -> snapshot artifact -> WSL research bundle -> isolated shadow artifact。代码合入不等于训练、数据库写入、服务重启或 capability 激活。

回滚只需停止消费指定 bundle ref/hash 并删除未发布的临时目录；已完成 immutable bundle 和报告保留为研究证据，不覆盖、不改写。由于不修改现有业务排名，回滚后荐股继续使用原 `selection_effective_rank`，无需恢复 Selection/Paper/模拟盘状态。

## 13. Production Gates / 生产门禁

本设计不新增人工门禁或审批。下表区分后续执行动作，不构成当前设计阶段授权：

| 项目 | 设计状态 | 说明 |
|---|---|---|
| production DDL | `noop` | 不新增 schema/migration |
| production DML | `pending_batch_b_execution` | 范围已冻结为 §9 Batch B 既有 Advisory table allowlist；实现后先 DEV 验证，再按具体生产目标执行；不是角色或业务审批 |
| production DB read | `configured_no_extra_gate` | 读取 `.env` 中既有数据库配置，不增加人工审批或第二套 DSN |
| backend dependency | `noop` | 设计复用 WSL 现有 LightGBM/PyArrow；如实现发现缺依赖须另行报告 |
| frontend dependency | `noop` | 无前端范围 |
| service restart | `noop` | 离线 CLI/WSL 训练，不控制用户服务 |
| runtime activation | `noop` | 不启用模型 consumer |
| package approval | `noop` | 不做策略包二次准入 |
| human approval/role | `noop` | 无角色或审批状态机 |

SEALED/hash/PIT/WSL/bundle 校验是数据与程序正确性合同，不是人工门禁；输入正确时可确定性通过。样本不足只影响模型 capability，不阻断现有荐股业务。

## 14. Design Acceptance Matrix / 设计验收矩阵

本矩阵只表示详细设计已经闭合；`design_ready` 不表示源码、数据、模型或影子推理已经完成。代码 PR 必须替换为真实 implementation refs 和测试证据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | `backend/services/advisory_modeling/style_profile.py` | `backend/tests/advisory_modeling/test_contracts_and_features.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-202 | §2、§3、§5.1 | artifact: `docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md` | design_ready | none |
| F-203 | §6.2、§6.3.1、§9 Batch B | artifact: `docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` | design_ready | none |
| F-204 | `backend/services/advisory_modeling/feature_schema.py`、`feature_snapshot.py` | `backend/tests/advisory_modeling/test_contracts_and_features.py`、`test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-205 | `backend/services/advisory_modeling/label_policy.py` | `backend/tests/advisory_modeling/test_contracts_and_features.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-206 | `backend/services/advisory_modeling/feature_builder.py`、`feature_schema.py` | `backend/tests/advisory_modeling/test_contracts_and_features.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-207 | `backend/services/advisory_modeling/training_view.py` | `backend/tests/advisory_modeling/test_split_experiment_regime.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-208 | `backend/services/advisory_modeling/contracts.py` | `backend/tests/advisory_modeling/test_split_experiment_regime.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-209 | §6.7 | artifact: `docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` | design_ready | none |
| F-210 | `backend/services/advisory_modeling/bundle_store.py` | `backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-211 | `backend/services/advisory_modeling/shadow_inference.py` | `backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-212 | §7.3 | artifact: `docs/architecture/advisory_phase0b_candidate_quality_modelability_f1_design_20260731.md` | design_ready | none |
| F-213 | §7.1-§7.2 | artifact: `docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` | design_ready | none |
| F-214 | §7.2 | artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-215 | `backend/services/advisory_modeling/errors.py`、全部 frozen contracts | `python -m nox -s advisory_modeling_backend` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-216 | §6.9、§10.4 | artifact: `docs/architecture/advisory_phase2_phase3_short_rebound_reranker_f2_design_20260802.md` | design_ready | none |
| F-217 | §6.1、§6.9 | artifact: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` | design_ready | none |
| F-218 | `backend/services/advisory_modeling/contracts.py`、`bundle_store.py` | `backend/tests/advisory_modeling/test_split_experiment_regime.py`、`test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-219 | `backend/services/advisory_modeling/bundle_store.py` | `backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-220 | `backend/services/advisory_modeling/**` | artifact: `tests/aistock_validation/history/advisory_modeling/20260803_advisory_short_rebound_batch_a_contract_review.md` | batch_a_verified | none |
| F-221 | Batch A source diff contains no migration/repository/runtime writes | `python -m nox -s advisory_modeling_backend` | batch_a_verified | none |
| F-222 | `backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py` | `backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |
| F-223 | Batch A implementation and five review rounds | artifact: `tests/aistock_validation/history/advisory_modeling/20260803_advisory_short_rebound_batch_a_contract_review.md` | batch_a_verified | none |
| F-224 | `backend/services/advisory_modeling/label_policy.py`、`shadow_inference.py` | `backend/tests/advisory_modeling/test_contracts_and_features.py`、`test_artifacts_shadow_isolation.py` | batch_a_contract_verified_remaining_batches_design_ready | none |

## 15. 当前结论与下一步

本设计把首个可用模型功能压缩为一个明确垂直切片，没有恢复历史数据或建设通用平台。Batch A 的 style、feature/formula/query、label、split、experiment、regime、bundle、shadow-result 和 typed reason 合同已完成源码与五轮审核；它没有执行数据库读取、特征物化、WSL 训练或模型推理，因此不代表 Phase 2/3 整体完成。下一步是 Batch B 新多年 snapshot 与训练文件构建。任何代码合入、生产数据库读取/写入、依赖安装、服务控制或模型激活仍需按当时具体任务单独报告。
