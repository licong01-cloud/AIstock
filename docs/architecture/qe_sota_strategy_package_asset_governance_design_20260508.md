# QE SOTA 殿堂、StrategyPackage 资产治理、Seed 可复现性与模型库设计

> **2026-07-15 作废/取代声明**：本文已不再作为 LocalSIM / MiniQMT 模拟盘、Selection Center、Paper Trading v2 或未来实盘运行边界的实现依据。模拟盘平台唯一上位蓝图为 `docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md`。本文仅保留 QE / SOTA / alpha core 治理方向；凡把执行、风控、HMM 或 runtime variant 作为 StrategyPackage 内容的描述，均已作废。
> 若本文与唯一上位蓝图不一致，以上位蓝图为准；旧描述只能作为历史背景或迁移参考，不能指导新开发。

> **2026-05-20 边界更新**：本文中“不可变 Alpha Core”的定义仍保留；但关于 StrategyPackage 吸收组合策略、风控策略、执行策略、HMM runtime variant 的描述已被 `docs/architecture/strategy_package_platform_boundary_contract_20260520.md` 取代。新版边界要求 StrategyPackage 只绑定因子和模型 alpha core；日频/分钟/尾盘/HMM/ST PIT/event/risk/broker 通过平台 profile/policy/version/activation 管理。

日期：2026-05-08
状态：实施前设计草案
范围：QuantEvolver / QE、SOTA 殿堂、StrategyPackage、模型库、QE 数仓、Paper Trading v2、未来实盘候选治理

## 1. 摘要

QE 的核心目标是发现新的优秀组合：

```text
因子组合 × 模型规格/代码 × 训练超参 × seed policy × 策略参数 × 分钟线执行 × 风控/HMM
```

QE 不是为了复现旧结果而存在。复现能力是为了审计、验证和资产保全；探索能力才是 QE 的第一目标。因此，未来 AIstock 需要明确拆分四个职责：

```text
QE                         = 大范围探索和自动演进
QE 数仓                    = 永久保存所有研究事实和分析数据
SOTA 殿堂                  = 人工评审、晋级、复测、治理工作台
StrategyPackage            = Selection/Paper/未来实盘候选可复用的标准策略资产
```

建议不要新引入独立的 `AlphaAssetBundle` 模块。更务实的方案是增强现有 `StrategyPackage`，让它吸收 Alpha 资产包能力：锁定模型资产、锁定因子资产、feature schema、训练 recipe、seed policy、artifact manifest、validation run、资产生命周期等。

未来 SOTA 殿堂不应由 QE 自动写入。QE 自动演进只在任务层面标识当前表现最好的组合或候选组合；任何单次实验、自定义演进 loop、标准演进 loop，都只提供“手工加入 SOTA 殿堂”的按钮。用户点击确认后，组合才进入 SOTA 殿堂的评审/晋级流程。

加入 SOTA 殿堂的组合，必须通过“原始配置复测”后，才可以进入 Paper v2 和未来实盘候选链路。进入 SOTA 殿堂和 Paper v2 后，模型和因子是锁定的 alpha core；原始策略、分钟线执行、HMM、风控等配置要保留为 baseline，但允许创建自定义 runtime variant 做进一步验证。

## 2. 核心决策

### 2.1 QE 自动发现不等于资产晋级

未来 QE 自动演进只做研究层判断：

```text
QE loop 完成
  -> 计算指标
  -> 标识 task 内当前最佳 loop / 最佳组合
  -> 可选地产生 SOTA candidate 信号
  -> 不自动加入正式 SOTA 殿堂
  -> 不自动进入 Paper v2
```

所有进入 SOTA 殿堂的动作都必须人工确认，适用于：

- 单次自定义 QE 实验；
- 标准 QE evolution loop；
- 自定义 evolution loop；
- 未来 multi-alpha loop；
- 被 QE 自动 evaluator 判断为优秀的 loop。

### 2.2 SOTA 殿堂升级为策略资产治理工作台

SOTA 殿堂未来应是唯一的用户侧策略资产晋级入口，但不应该是所有文件和指标的物理存储层。

SOTA 殿堂职责：

- 展示 QE 自动发现和人工加入的候选组合；
- 支持人工批准/拒绝；
- 触发资产冻结；
- 触发强制原始配置复测；
- 生成或更新 StrategyPackage；
- 管理 Paper-ready 状态；
- 展示长期复测、模拟盘、未来实盘候选状态。

底层权威对象应该是增强后的 `StrategyPackage`，不是旧的 `qe_sota_registry` 记录。

### 2.3 增强 StrategyPackage，不新建 AlphaAssetBundle

概念上，`AlphaAssetBundle` 代表：

```text
因子集合 + 模型规格 + 训练 recipe + seed + 权重 + schema + 研究证据
```

`StrategyPackage` 代表：

```text
Alpha 资产 + 组合策略 + 风控策略 + 执行策略 + Paper/未来实盘入口契约
```

对当前 AIstock 来说，引入独立 `AlphaAssetBundle` 会造成第二套资产系统，容易出现权威不清：

```text
SOTA 一套资产
StrategyPackage 一套资产
Paper v2 又消费另一套引用
```

因此推荐：

```text
StrategyPackage v2 = 当前 StrategyPackage + AlphaAssetBundle 能力
```

保留 StrategyPackage 作为 Selection Center、Paper v2 和未来实盘候选的唯一标准资产对象。

### 2.4 Paper v2 不直接消费 QE loop

Paper v2 和未来实盘候选不应直接选择任意 QE 实验或 loop。它们只应选择满足门槛的 StrategyPackage：

- 已经人工加入 SOTA 殿堂或经过等价人工晋级；
- 模型/因子 core asset 已冻结；
- 原始配置复测通过；
- manifest 和 asset hash 完整；
- 模型推理健康检查通过；
- 执行策略已验证；
- 风控和数据合约完整；
- 不依赖可清理的 QE workspace 路径。

## 3. 当前状态理解

### 3.1 当前 SOTA 殿堂

当前 `/quantevolver/evolution/sota` 页面本质是 QE SOTA loop 排行榜。它读取：

- `/api/v1/quantevolver/evolution/leaderboard`；
- `/api/v1/quantevolver/evolution/sota`。

后端聚合 `qe_sota_registry`、`qe_evolution_loops`、`qe_evolution_tasks`，提取 `metrics_json` 并排序。这适合作为研究发现页，但还不是资产治理系统。

当前不足：

- `is_sota` 只表达表现优秀，不表达 Paper-ready；
- 资产同步状态不完整；
- 模型权重、因子 schema、seed、代码 hash、执行参数、复测状态没有完整治理；
- SOTA loop 可能仍依赖会被清理的 worker workspace；
- SOTA loop 不一定具备严格 retrain reproducibility 或 live inference readiness。

### 3.2 当前 StrategyPackage

现有 StrategyPackage 方向是正确的。它已经承担：

- `manifest_json` 和 `manifest_sha256`；
- QE experiment/loop 来源；
- 因子集合和模型引用；
- 策略配置；
- universe / portfolio / execution / minute execution policy；
- validated execution policy；
- model state；
- Selection Center 和 Paper v2 入口。

问题不是 StrategyPackage 概念错误，而是资产完整性不足。需要增强它，使其真正保存和校验被晋级组合的模型、因子、schema、训练配置、seed、权重和验证证据。

## 4. SOTA 殿堂 v2 工作流

### 4.1 QE 自动标识

QE 自动演进仍然可以保留 task 内最佳组合逻辑，例如写入：

```text
task_best_loop_id
task_best_score
best_by_metric
candidate_reason
candidate_score_breakdown
```

但它不自动写入正式 SOTA 殿堂。

### 4.2 手工加入入口

所有完成的 QE 结果都应该有手工操作：

```text
加入 SOTA 殿堂
```

适用来源：

- 单次 QE 实验；
- 标准演进 loop；
- 自定义演进 loop；
- 多 alpha 演进 loop；
- backtest-only rerun，但前提是源 lineage 完整。

点击确认后，应创建 promotion/review 记录，而不是立即 Paper-enabled。

### 4.3 推荐状态机

```text
AUTO_CANDIDATE          QE 自动标识的 task 内候选
REVIEW_PENDING          用户点击加入 SOTA 殿堂，等待评审
REVIEW_REJECTED         人工拒绝，保留审计记录
SOTA_APPROVED           人工批准进入 SOTA 殿堂
ASSET_FREEZING          正在复制和 hash 模型/因子/配置资产
ASSET_FROZEN            core asset 已进入 AIstock 受控资产库
ORIGINAL_RETESTING      原始配置复测中
ORIGINAL_RETEST_PASSED  原始配置复测通过
ORIGINAL_RETEST_FAILED  原始配置复测失败
PAPER_CANDIDATE         具备 Paper readiness 检查资格
PAPER_ENABLED           可被 Paper v2 选择
PAPER_VALIDATED         模拟盘验证通过
LIVE_CANDIDATE          未来实盘候选，非实盘 armed 状态
RETIRED                 退役，不再可选，但不删除历史资产
```

`is_sota=true` 不能再作为完整生命周期字段，只能作为候选/历史标签。

### 4.4 人工评审清单

进入 `SOTA_APPROVED` 前，评审人至少应看到并确认：

- source experiment / task / loop；
- model family / type / spec；
- factor list 和 factor source；
- seed policy 与 seed 完整性；
- backtest window 和 data version；
- IC、RankIC、ICIR、return、Sharpe、max drawdown；
- turnover、cost drag、最大持股数、平均持股数；
- 执行算法和成本参数；
- ST PIT、停牌、涨跌停、risk policy 证据；
- 数据质量和可复现性等级；
- 结果来源是 training、backtest-only 还是 rerun。

## 5. 增强版 StrategyPackage 设计

### 5.1 不可变 Alpha Core

一旦 StrategyPackage 进入 SOTA 殿堂并冻结，以下内容对该 package version 不可变：

```text
factor set
factor order
factor schema hash
factor code / version / hash
model specification
model code hash
model architecture params
training recipe
seed policy and seed value(s)
trained model weights
fitted preprocessors / normalizers
feature schema and feature order
label configuration
train / valid / test split identity
source data snapshot identity
```

任意一项改变，都应创建新的 package version 或新的 StrategyPackage。

### 5.2 Baseline Runtime Configuration

原始 QE runtime 配置要作为 baseline 永久保留：

```text
strategy parameters
portfolio parameters
risk policy
minute execution policy
HMM settings
cost model
rebalance policy
selection runtime defaults
```

这些不属于锁定的 alpha core，但属于原始证据上下文，必须可查询。

### 5.3 可控 Runtime Variant

组合晋级后，可以允许非 core 设置的自定义版本：

- strategy TopK / n_drop / threshold；
- 分钟线执行算法，例如 V25、V25.1、V26；
- 执行成本、滑点、tolerance；
- HMM on/off 或不同 HMM snapshot；
- 行业黑名单、风险 overlay；
- 资金规模和容量约束。

规则：

- variant 不得静默修改 frozen package manifest；
- 每个 variant 都有 `runtime_variant_id` 和 hash；
- 每个 variant 进入 Paper 前必须单独验证；
- Selection/Paper run 必须记录实际使用的 variant hash；
- variant 不能改变模型权重或因子 schema。

### 5.4 建议增强字段

增强现有 StrategyPackage，不新建平行系统。

`strategy_pkg.package` 可增加或派生：

```text
lifecycle_status
promotion_status
paper_eligibility_status
live_candidate_status
core_asset_sha256
core_asset_manifest_json
source_qe_archive_run_id
source_sota_review_id
frozen_at
retired_at
retire_reason
```

强化 `strategy_pkg.package_asset` 用法：

```text
asset_type: model_weight | factor_code | factor_schema | feature_order | train_config | preprocessor | prediction_schema | execution_config | risk_policy | validation_report
asset_uri
asset_sha256
asset_size_bytes
asset_role
retention_class
protected_asset
source_uri
source_sha256
metadata
```

可新增表：

```text
strategy_pkg.promotion_review
strategy_pkg.package_validation_run
strategy_pkg.package_runtime_variant
strategy_pkg.model_version
strategy_pkg.asset_integrity_check
```

## 6. 复测与验证设计

### 6.1 强制原始配置复测

被加入 SOTA 殿堂的组合，在进入 Paper v2 或未来实盘候选前，必须通过原始配置复测。

复测使用：

- 被晋级的模型/因子组合；
- 原始 strategy config；
- 原始 minute execution config；
- 原始 risk / HMM config；
- 尽可能使用原始 backtest window 和 data snapshot；
- 使用已复制到 AIstock 受控资产库的 frozen artifacts，不依赖可清理 QE workspace。

目的：

```text
证明这个被晋级的资产完整、可运行，并且与源 QE 证据一致或可解释地接近。
```

它不是为了证明未来收益，而是资产完整性和 baseline reproducibility gate。

### 6.2 复测模式

SOTA 殿堂应支持多种 validation mode，每次保存为独立 validation run。

#### Mode A：锁定资产原始复测

```text
retrain: false
weights: frozen promoted weights
data: original snapshot or same data version
config: original QE strategy / execution / risk config
```

回答：

```text
这个被晋级资产现在是否还能完整运行并产生可比行为？
```

#### Mode B：同配置重训复测

```text
retrain: true
seed: recorded fixed seed or declared seed policy
data: original data version
config: same training recipe and factor schema
```

回答：

```text
训练过程是否可复现？是否高度依赖随机性？
```

这会产生新 model version，不能覆盖原始权重。

#### Mode C：最新数据集固定权重延展验证

```text
retrain: false
weights: frozen promoted weights
data: latest compatible dataset
feature schema: must match exactly
```

回答：

```text
旧模型权重在新增数据区间是否仍有信号？
```

#### Mode D：最新数据集重训验证

```text
retrain: true
data: latest dataset
recipe: frozen training recipe
seed_policy: fixed or multi_seed
```

回答：

```text
这个模型/因子 recipe 在更新数据后重新训练是否仍有效？
```

#### Mode E：Walk-forward 滚动训练验证

```text
multiple windows
new model version per window
same model spec and factor schema
explicit seed policy
```

回答：

```text
这个组合是否适合未来周期性滚动训练？
```

#### Mode F：Runtime Variant 回测

```text
model/factor core: locked
strategy/execution/HMM/risk: selected variant
retrain: false by default
```

回答：

```text
同一 alpha core 下，不同策略、分钟线执行或 HMM 配置是否能改善结果？
```

### 6.3 复测结果不能覆盖原始指标

原始 QE 指标、原始配置复测指标、最新数据验证指标、滚动训练指标、Paper 指标、未来实盘指标必须分别存储。

推荐 validation-run 字段：

```text
validation_run_id
package_id
manifest_sha256
runtime_variant_id
validation_type
retrain_mode
model_version_id
seed_policy
random_seed
source_data_version
target_data_version
backtest_start
backtest_end
status
metrics_json
artifact_manifest_json
reproducibility_level
created_by
created_at
completed_at
```

## 7. Seed 与可复现性方案

### 7.1 `seed: None` 的含义

`seed: None` 表示有效训练配置中没有记录固定随机种子。它不直接代表训练不充分，但代表相同 nominal config 重训时可能产生不同权重和指标。

原因包括：

- 随机初始化；
- DataLoader 顺序；
- GPU nondeterminism；
- Python / NumPy / Torch / LightGBM / XGBoost / CatBoost 内部随机性；
- 多进程 worker seed 未固定。

历史实验如果没有 seed 和 artifact hash：

```text
strict retrain reproducibility: 不保证
frozen-weight inference reproducibility: 只有权重、feature schema、预处理资产完整保存时才可能
```

缺失 seed 不能事后可靠推断，只能补录为 legacy metadata：

```text
seed_policy = unset_legacy
random_seed = null
reproducibility_level = audit_only 或 artifact_only
```

### 7.2 Seed 是审计工具，不是单独 alpha 来源

不应把“找到一个最好 seed”作为主要 alpha 目标。稳定 alpha 不应依赖一次幸运 seed。

推荐策略：

- discovery 阶段：允许 random 或 multi-seed，但记录实际使用 seed；
- candidate 阶段：做 multi-seed stability；
- promotion 阶段：冻结选中的 weight artifact，并记录 seed；
- Paper 阶段：使用 frozen weights，或使用明确批准的 rolling retrain policy；
- rolling retrain 阶段：每次重训记录 seed、deterministic flags、数据窗口和 model version。

### 7.3 必须记录的 Seed Contract

每次未来模型训练 trial 应记录：

```text
seed_policy: fixed | multi_seed | random_logged | unset_legacy
random_seed
seed_sequence
python_hash_seed
numpy_seed
torch_seed
torch_cuda_seed
lightgbm_seed
xgboost_random_state
catboost_random_seed
dataloader_worker_seed_policy
deterministic_algorithms_enabled
cudnn_deterministic
cudnn_benchmark
library_versions
hardware_context
```

对于 PyTorch 类模型，应固定 Python、NumPy、Torch CPU、Torch CUDA、DataLoader workers。如果没有开启 deterministic algorithms，也必须显式记录。

### 7.4 Seed 稳定性指标

对晋级候选，建议计算：

```text
metric_mean_by_seed
metric_std_by_seed
worst_seed_metric
best_seed_metric
seed_sensitivity_score
rank_stability
factor_importance_stability
selection_overlap_by_seed
```

如果组合高度依赖某个 seed，标记：

```text
seed_fragile = true
```

这种 package 可以留在研究/SOTA 殿堂，但不应默认 Paper-enabled，除非人工显式 override。

### 7.5 best_epoch=0 的解释

`best_epoch=0` 是诊断信号，不是自动失败。可能原因：

- early stopping 选择初始/基线 epoch；
- validation loss 一开始就恶化；
- learning rate 或模型结构不合适；
- 因子/label 信号弱；
- 训练数据或归一化存在问题；
- 某个随机初始化刚好最好。

对随机神经网络模型，如果 `best_epoch=0`，晋级时应加强人工评审和复测。

## 8. 模型库设计

### 8.1 模型库不只是验证通过模型

QE 需要广泛模型搜索空间。模型库应包含：

- 探索用模板和模型规格；
- RD-Agent 生成候选；
- 人工 research candidate；
- validated model spec；
- promoted model artifact；
- retired / quarantined / training_failed 记录。

这些模型不能在 Paper v2 中同等可选。

### 8.2 四层模型身份

建议拆分：

```text
ModelTemplate   = 可复用模型族模板，例如 LGB、CatBoost、LSTM、Transformer
ModelSpec       = 代码 + 架构 + 允许的超参/search space
ModelTrial      = 一次训练尝试，包含数据、因子、超参、seed、指标
ModelArtifact   = 某次 trial 保存的权重、预处理器、prediction schema
```

规则：

- 不同模型代码 -> 不同 `ModelSpec`；
- 相同代码但不同架构 -> 不同 `ModelSpec` 或 spec version；
- 相同 spec 但不同超参/因子/split/seed -> 不同 `ModelTrial`；
- 不同权重 -> 不同 `ModelArtifact`；
- 被晋级的 artifact -> 绑定到 StrategyPackage。

### 8.3 模型库生命周期

推荐状态：

```text
template
research_candidate
rdagent_candidate
validated_spec
promoted_artifact
paper_candidate
paper_enabled
quarantined
training_failed
retired
```

失败或价值较低模型不建议 hard delete。应隐藏出默认选择器，但保留审计、去重和经验学习价值。

### 8.4 QE 可选模型范围

QE 选择模型时，应面向搜索空间：

```text
model templates
research candidates
RD-Agent candidates
validated specs
explicit experimental models, if enabled
```

QE 不应只限于历史验证模型，因为 QE 的目标是探索。

### 8.5 Paper 可选模型范围

Paper v2 不直接选择模型库条目。Paper v2 选择的是包含 promoted model artifact 和 locked factor schema 的 StrategyPackage。

```text
QE selection target     = model specs / search spaces
Paper selection target  = paper-ready StrategyPackages
```

### 8.6 模型库、StrategyPackage、QE 数仓边界

```text
Model Library
  保存可复用模型 spec、代码 hash、生命周期、默认 search space、promoted artifacts。

StrategyPackage
  保存被晋级组合的模型/因子 core，以及完整策略资产和 Paper/未来实盘契约。

QE Warehouse
  保存所有 trial 事实，包括失败、较弱、随机、不晋级的运行。
```

数仓可以记录每一次 model trial；模型库保留可复用候选和 promoted assets；StrategyPackage 指向用于策略的精确模型 artifact。

### 8.7 模型库底层规范化存储

模型库不应继续只依赖一张扁平 `aistock_model_catalog` 表。推荐目标结构是独立 `model_registry` schema，至少包含：

```text
model_registry.model_template
model_registry.model_spec
model_registry.model_trial
model_registry.model_artifact
model_registry.model_lifecycle_event
```

关系：

```text
model_template 1 -> N model_spec
model_spec     1 -> N model_trial
model_trial    1 -> N model_artifact
model_artifact N -> N strategy_pkg.package_asset / StrategyPackage
```

#### model_template

保存模型族能力，不保存某次训练结果：

```text
template_id
family                  # tree / boosting / neural_ts / tabular_deep / transformer
model_type              # LGBModel / XGBoost / CatBoost / LSTM / GRU / Transformer
display_name
description
task_type               # rank / regression / classification
supported_freq          # day / 1min / mixed
supported_input_shape   # tabular / sequence_10d / sequence_30d
train_backend           # qlib / sklearn / torch / tabpfn
default_search_space
default_train_budget
seed_capability         # fixed / multi_seed / random_logged / unsupported
deterministic_support   # full / partial / none
gpu_required
lifecycle_status        # active / experimental / deprecated / retired
```

#### model_spec

保存可训练模型规格：代码、结构、输入输出契约和搜索空间。

```text
spec_id
template_id
spec_version
model_name
model_type
code_ref
code_text
code_sha256
architecture_config
architecture_sha256
hyperparam_schema
default_hyperparams
search_space_json
input_contract_json
output_contract_json
feature_schema_requirements
label_requirements
dependency_versions
source_type             # builtin / rdagent_sync / manual / imported
source_task_id
source_loop_id
lifecycle_status        # template / research_candidate / validated_spec / quarantined / retired
qe_selectable
qe_selectability_reason
created_at
updated_at
```

#### model_trial

保存一次训练试验事实。相同模型规格、不同因子/数据/split/超参/seed 都是不同 trial。

```text
trial_id
spec_id
qe_run_id
qe_experiment_id
qe_task_id
qe_loop_id
factor_set_hash
factor_list_ordered
feature_schema_hash
data_context_id
dataset_version
label_config_hash
train_start
train_end
valid_start
valid_end
test_start
test_end
train_config_json
hyperparams_json
seed_policy
random_seed
seed_sequence
deterministic_flags_json
status                  # succeeded / failed / interrupted / invalid
failure_reason
best_epoch
total_epochs
train_loss_final
val_loss_final
training_curves
ic
rank_ic
icir
annualized_return
sharpe
max_drawdown
turnover
cost_drag
score_total
created_at
completed_at
```

#### model_artifact

保存具体可复用资产，包括权重和推理所需辅助文件。

```text
artifact_id
trial_id
artifact_type           # weights / preprocessor / feature_order / feature_schema / prediction_schema / checkpoint / params
artifact_uri
artifact_sha256
artifact_size_bytes
feature_schema_hash
feature_order_hash
preprocessor_hash
model_format            # pkl / pt / json / txt / qlib_recorder / tar
retention_class         # temporary / archived / promoted / protected
protected_asset
artifact_status         # present / missing / corrupted / expired
created_at
validated_at
metadata_json
```

#### model_lifecycle_event

记录治理变更，替代无审计的直接删除。

```text
event_id
object_type             # template / spec / trial / artifact
object_id
from_status
to_status
reason
operator
context_json
created_at
```

### 8.8 `aistock_model_catalog` 的过渡定位

当前 `aistock_model_catalog` 混合了模型规格、训练结果、SOTA 标记、代码、诊断、workspace 路径和指标。短期不建议立刻废弃，而应降级为兼容层：

```text
aistock_model_catalog = 旧 API 兼容表 / 聚合视图
model_registry.*      = 新权威结构
```

短期补充治理字段：

```text
model_role              # template / spec / trial / artifact_legacy
lifecycle_status
qe_selectable
qe_selectability_reason
paper_selectable         # 固定 false；Paper 选择 StrategyPackage，不选模型
seed_policy
random_seed
determinism_level
code_sha256
architecture_sha256
model_spec_sha256
feature_schema_hash
artifact_status
protected_asset
quarantine_reason
retired_at
```

删除操作应改为高权限治理动作。普通页面默认提供：

```text
hide_from_qe
quarantine
retire
restore_to_research_candidate
```

只有确认没有 StrategyPackage / Paper / 归档引用时，管理员才允许物理删除。

### 8.9 模型库页面展示设计

模型库页面建议从单一列表拆为五个视图。

#### 视图 A：模型搜索空间

面向 QE 创建实验，展示 `ModelTemplate + ModelSpec`。

核心列：

```text
模型族
模型规格
输入形态
支持频率
默认搜索空间
训练成本
GPU需求
Seed支持
确定性支持
QE可选状态
生命周期
最近验证摘要
```

默认隐藏：

```text
training_failed
quarantined
retired
runtime_broken
data_incompatible
```

#### 视图 B：训练试验

面向研究复盘，展示 `ModelTrial`。

核心列：

```text
trial_id
模型规格
因子组合 hash
数据版本
seed
训练状态
best_epoch
IC / RankIC / ICIR
年化 / Sharpe / 回撤
训练耗时
QE来源
失败原因
```

筛选项：

```text
模型族
因子组合
seed_policy
数据版本
训练状态
best_epoch=0
高回撤
高seed敏感
```

#### 视图 C：模型资产

面向资产治理，展示 `ModelArtifact`。

核心列：

```text
artifact_id
模型规格
trial_id
权重状态
feature_schema_hash
feature_order_hash
sha256
retention_class
protected_asset
是否绑定 StrategyPackage
是否 Paper 可用
```

可执行动作：

```text
校验 hash
复制到受控资产库
绑定 StrategyPackage
标记 protected
标记 corrupted
```

#### 视图 D：已晋级 / Paper 相关

展示已经被 SOTA 殿堂或 StrategyPackage 使用的模型资产。

核心列：

```text
StrategyPackage
manifest_sha256
model_artifact_id
factor_schema_hash
原始复测状态
Paper状态
最近Paper表现
模型新鲜度
```

该视图只展示关系，不允许直接把模型送入 Paper。Paper 仍只选择 StrategyPackage。

#### 视图 E：治理 / 清理

展示失败、重复、退役、缺资产模型。

分组：

```text
training_failed
missing_code
missing_artifact
seed_unset_legacy
quarantined
retired
duplicate_spec
```

### 8.10 QE 实验中的模型选择范围

QE 创建实验时不应简单选择“历史训练模型”。应按模式选择不同对象。

#### 模式 A：训练新模型

默认模式。选择对象是：

```text
ModelSpec / ModelSearchSpace
```

可选范围：

```text
template
research_candidate
rdagent_candidate
validated_spec
experimental, if explicitly enabled
```

默认排除：

```text
training_failed
runtime_broken
data_incompatible
quarantined
retired
paper_only_artifact
```

UI 必须提示：

```text
当前选择的是模型规格/搜索空间，不是复用历史权重。QE 会基于本次因子组合重新训练模型。
```

#### 模式 B：自动搜索模型池

用户选择模型池，而不是单一模型。

示例模型池：

```text
快速树模型池：LGB + XGB + CatBoost
时序神经网络池：LSTM + GRU + TCN
深度探索池：Transformer + TabPFN + NN variants
稳健基线池：LGB + Ridge + shallow MLP
RD-Agent候选池
```

QE 生成：

```text
factor_set × model_spec × hyperparams × seed
```

#### 模式 C：复用权重 / 只回测

特殊模式，不是默认探索模式。选择对象是：

```text
ModelArtifact
```

必须满足：

```text
feature_schema_hash 完全匹配
feature_order_hash 完全匹配
preprocessor_hash 完整
模型权重存在
artifact hash 校验通过
```

用途：

- 验证执行策略；
- 验证成本参数；
- 验证 HMM / 风控；
- 固定信号下做 Paper-like 回测。

不同因子组合不能默认复用旧权重。

### 8.11 QE 模型选择 UI

QE 创建向导中的模型页建议分三层。

第一层：选择模式。

```text
自动模型搜索池（推荐）
手工选择模型规格
复用已训练权重，只做回测
```

第二层：选择范围。

如果是自动模型搜索池：

```text
快速筛选
稳健树模型
时序神经网络
深度探索
RD-Agent候选
人工实验模型
```

如果是手工选择模型规格，展示 `ModelSpec` 表：

```text
模型规格
模型族
输入要求
支持当前因子
Seed支持
确定性等级
训练成本
最近trial数量
最近中位IC
最近最差回撤
失败率
生命周期
```

如果是复用已训练权重，展示 `ModelArtifact` 表：

```text
artifact
所属 StrategyPackage / Trial
feature_schema_match
权重状态
原始因子数
原始训练窗口
原始 seed
hash 状态
是否可用于当前因子组合
```

第三层：配置搜索预算。

```text
seed policy
每个模型最大 trial 数
超参搜索预算
训练时间上限
GPU / CPU 选择
早停策略
失败模型跳过策略
```

### 8.12 模型兼容性检查 API

QE 在选择因子后，应调用兼容性 API，而不是只按 `model_type` 或 `is_sota` 过滤。

```text
POST /api/v1/model-registry/compatible-specs
```

输入：

```text
factor_list
factor_schema
freq
label_horizon
data_split
training_mode
```

输出：

```text
spec_id
compatible
reason
required_input_shape
estimated_train_cost
seed_support
determinism_level
recommended_budget
warnings
```

典型原因：

```text
compatible=false, reason=requires_sequence_10d_but_current_features_are_tabular
compatible=false, reason=requires_gpu
compatible=false, reason=feature_count_too_small
compatible=true, warning=seed_unstable_model_family
```

这个 API 是 QE 模型选择、模型库页面可选状态、SOTA 晋级健康检查的共同基础。
## 9. 数据与资产存储边界

### 9.1 QE Runtime Tables

QE runtime 表用于运行态和 UI 状态，可清理：

```text
qe_experiments
qe_evolution_tasks
qe_evolution_loops
workspace paths
latest status
UI summaries
retry/debug state
```

它们不是长期资产库。

### 9.2 QE 数仓

QE 数仓永久保存研究事实：

```text
configs
metrics
curves
positions / trades summaries
model trials
factor stats
raw payload snapshots
artifact manifests
reproducibility manifests
failure reasons
```

数仓主要用于分析、排序、学习和审计，不应承担所有大模型权重的永久保全，除非该 artifact 被晋级。

### 9.3 StrategyPackage 资产库

被晋级 StrategyPackage 需要长期保护资产：

```text
model weights
preprocessors
feature schema
factor code / schema / order
frozen config
execution / risk policy manifests
validation reports
```

这些资产不能因为 QE 实验或 workspace 清理而删除。

### 9.4 模型库

模型库保存模型知识和模型资产索引：

```text
model template / spec / version
code hash
architecture hash
hyperparameter search space
trial summaries
artifact refs
lifecycle status
```

模型库不直接定义 paper-ready 策略；paper-ready 属于 StrategyPackage 状态。

## 10. SOTA 殿堂 UI 设计

### 10.1 主 Tab

建议：

```text
自动候选
待评审
已批准 SOTA
资产冻结中
需要复测
Paper 候选
Paper 已启用
已退役
```

### 10.2 行级操作

完成的 QE experiment / loop 可提供：

```text
查看 QE 诊断
查看源配置
加入 SOTA 殿堂
拒绝候选
冻结资产
运行原始配置复测
运行最新数据验证
生成 StrategyPackage
创建 runtime variant
运行 Paper readiness
启用 Paper v2
退役
```

操作必须按状态 gating。例如 `启用 Paper v2` 不能在原始复测通过前出现。

### 10.3 证据展示

每个已晋级组合显示：

- source QE run / loop；
- package id 和 manifest hash；
- model artifact status；
- factor schema status；
- seed policy；
- 原始指标；
- 复测指标；
- 最新数据验证指标；
- Paper 状态；
- runtime variants；
- asset integrity status；
- cleanup protection status。

## 11. Paper v2 和未来实盘候选准入门槛

StrategyPackage 可以被 Paper v2 选择的条件：

```text
lifecycle_status in PAPER_CANDIDATE or PAPER_ENABLED
core model/factor assets are frozen
manifest_sha256 exists
asset manifest hashes pass
original-config retest passed
selection inference health check passed
minute execution policy is validated
risk / ST PIT / suspend / limit contracts are complete
no disposable QE workspace dependency remains
```

未来 live-candidate 门槛应更严格，并且独立于 Paper v2：

```text
Paper 验证周期通过
回撤和换手约束通过
最新数据验证通过
如使用滚动训练，rolling retrain policy 已评审
人工审批记录完整
kill-switch / live controls 另行设计
```

本文不设计实盘交易激活、broker 接入、QMT armed 或真实下单流程。

## 12. 晋级后的 Runtime 自定义

用户需求是：进入 SOTA 殿堂和 Paper v2 后，只有模型和因子锁定，其他配置允许自定义尝试。

推荐拆分：

### 12.1 Locked Core

```text
model artifact
model code / spec
factor list
factor order
factor schema
feature preprocessing required by the model
```

### 12.2 Baseline But Variant-Capable

```text
strategy parameters
portfolio constraints
minute execution algorithm
minute execution parameters
HMM settings
risk overlays
cost / slippage assumptions
capital / capacity assumptions
```

### 12.3 Variant 规则

- 原始配置永远作为 baseline 保留；
- 自定义 runtime config 创建 variant record；
- variant 独立验证；
- Paper run 记录精确 variant hash；
- 成功 variant 可由人工批准成为该 package 的默认 Paper runtime；
- 改变模型或因子 core 必须创建新 package version，而不是 variant。

## 13. 推荐实施阶段

### Phase 0：文档和术语对齐

- 采纳本文作为目标设计。
- UI 文案区分自动候选、正式 SOTA、Paper-ready package。
- 停止把 QE 自动 SOTA 判断描述为自动加入 SOTA 殿堂。

### Phase 1：手工 SOTA 晋级

- 在 QE experiment 和 loop 页面增加“加入 SOTA 殿堂”。
- QE 自动逻辑只写 task-level best / candidate。
- 增加 promotion review 记录和 lifecycle status。
- SOTA 殿堂展示 pending / rejected / approved。

### Phase 2：StrategyPackage 资产冻结

- 扩展 package asset manifest 覆盖范围。
- 将晋级模型、因子、配置复制到 AIstock 受控资产库。
- 记录 sha256、size、source、protected_asset。
- 阻止 QE cleanup 删除已晋级资产。

### Phase 3：强制原始复测

- 增加 package validation run。
- 实现 locked-asset original retest。
- Paper v2 eligibility 依赖 retest pass。
- 复测 artifact manifest 和 metrics 独立保存。

### Phase 4：Seed Contract 和 Model Trial 记录

- QE request/config/model training output 增加 seed policy。
- seed 映射到 LGB / XGB / CatBoost / TabPFN / PyTorch。
- 记录 deterministic flags 和 package versions。
- 新 QE run 填充 `run_model_trial` / model trial 数据。

### Phase 5：模型库治理

- 拆分 model template / spec / trial / artifact 语义。
- 增加 lifecycle / selectability 字段。
- failed / quarantined / retired 从默认选择器隐藏。
- 保持 QE 广泛模型搜索空间和 Paper-ready StrategyPackage 分离。

### Phase 6：Runtime Variants

- 增加 StrategyPackage runtime variants。
- 在锁定模型/因子 core 的前提下，允许 strategy / minute execution / HMM / risk 自定义。
- variant 进入 Paper 前必须验证。

### Phase 7：最新数据与滚动训练验证

- 增加 latest-dataset fixed-weight validation。
- 增加 latest-dataset retrain validation。
- 增加 walk-forward rolling validation。
- 增加 seed-stability 和 regime-stability scoring。

## 14. 测试和验收要求

每个阶段都应按风险等级做 L0-L5 验证。

最低测试：

- QE 自动 candidate 不会自动变成 approved SOTA；
- 手工 promotion 创建 review/audit 记录；
- rejected candidate 不可被 Paper 选择；
- asset freezing 复制文件并记录 hash；
- hash mismatch 阻止 Paper eligibility；
- original retest 是 Paper eligibility 前置条件；
- Paper v2 只列出 eligible StrategyPackage；
- runtime variant 不能改变 locked factor/model core；
- QE cleanup 保留 promoted package assets；
- missing seed 记录为 `unset_legacy`，不静默默认；
- fixed seed 正确传递到各模型类型；
- multi-seed stability metrics 可保存和展示；
- legacy experiments 可查询，但不标记为 strict retrain reproducible。

## 15. 待定问题

1. 正式 SOTA Hall 审批是否需要单人确认，还是允许 research-only 自动批准？
2. 原始配置复测通过后，`PAPER_CANDIDATE` 应采用哪些指标阈值？
3. latest-data validation 是 Paper 必选项，还是只作为 future live-candidate 门槛？
4. runtime variant 成功后是否自动成为默认 Paper runtime，还是必须人工批准？
5. 非晋级 QE artifact 在数仓归档后保留多久？
6. 哪些模型类型必须支持严格 deterministic training，哪些只能支持 logged-random reproducibility？

## 16. 最终目标架构

```text
QE Experiment / QE Evolution Loop
  -> 自动标识 task-level best / candidate，不自动加入 SOTA 殿堂
  -> QE 数仓归档所有研究事实
  -> 用户手工加入 SOTA 殿堂
  -> SOTA review 批准或拒绝
  -> StrategyPackage 冻结模型/因子 core 和 baseline config
  -> 原始配置复测验证资产完整性
  -> runtime variants 探索 strategy / execution / HMM / risk
  -> Paper v2 只选择 Paper-ready StrategyPackage
  -> 未来实盘候选复用同一套 canonical package 记录
```

这个设计同时满足两个目标：

- QE 保持探索自由，可以持续发现新的模型/因子/超参组合；
- Paper v2 和未来实盘候选获得资产完整性、可审计性、可复测性和治理边界。

---

# 附录 A：Claude Code 补充建议（跨方案整合分析）

> **作者**：Claude Code（Opus 4.7）
> **生成时间**：2026-05-08
> **来源参考**：`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §29-§32（含完整推导）
> **状态**：本附录为补充建议，**不修改本文档第 1-16 章主体内容**；如有冲突以主体（Codex 设计）为准。
>
> **附录目的**：
> (1) 把 QE 治理设计与 Paper v2 / vn.py 实盘路径明确协同；
> (2) 提出共享 Strategy Engine 层（建议在 StrategyPackage v2 之上叠加）；
> (3) 规范跨工具（Claude Code + Codex）协作；
> (4) 补充每 Phase 必备的测试用例与覆盖度要求；
> (5) 列出立即启动的工作清单。

## A.1 本附录与文档主体（第 1-16 章）的关系

主体设计覆盖：QE 治理 / 职责四分 / 生命周期状态机 / Master Seed Contract / Seed Fragility Scoring / Model Library 4 层（Template/Spec/Trial/Artifact）/ Frozen Alpha Core / Runtime Variants / Validation Modes A-F / 7 阶段实施路线。

**本附录补充**（不冲突、可叠加）：
- A.2：与 Paper v2 / vn.py / 未来实盘的三层架构定位
- A.3：共享 Strategy Engine 层（StrategyPackage v2 之上）的扩展建议
- A.4：跨工具（Codex + Claude Code）协作硬规范
- A.5：每 Phase 必备测试用例与新建测试矩阵清单
- A.6：未来架构演进的约定（新 alpha 类型 / 实盘切换 / 滚动训练接入）
- A.7：立即启动的优先级工作

## A.2 与 Paper v2 / vn.py 路径的三层架构

主体设计回答了"QE 资产怎么治理 + 怎么晋级到 Paper / 实盘"，但未涉及 Paper v2 实际执行路径与未来实盘 OEMS 接入。完整三层架构：

```
┌─────────────────────────────────────────────────┐
│ 治理层（本文档第 1-16 章 已设计）                 │
│  - SOTA 殿堂 / StrategyPackage v2 / Frozen Alpha │
│  - Model Registry / Seed Contract                │
│  - Validation Modes A-F                          │
└─────────────────────────────────────────────────┘
                       │
                       ▼ 输出 frozen manifest 给下层
┌─────────────────────────────────────────────────┐
│ 策略执行层（A.3 推荐补充）                        │
│  - 共享 Strategy Engine（plain Python，无 Qlib /  │
│    vn.py 依赖）                                  │
│  - 输入：StrategyPackage v2 manifest + score +    │
│    current_positions                             │
│  - 输出：List[OrderIntent]                        │
└─────────────────────────────────────────────────┘
                       │
        ┌──────────────┼───────────────┐
        ▼              ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ QE Adapter   │ │ Paper Adapter│ │ Live Adapter │
│ Qlib YAML +  │ │ trading_core │ │ trading_core │
│ delegate     │ │ + SimGateway │ │ + vnpy_xt    │
│ (现有 QE)    │ │ (vn.py 库)   │ │ (vnpy_xt)    │
└──────────────┘ └──────────────┘ └──────────────┘
        │              │                │
        ▼              ▼                ▼
   Qlib backtest    SimGateway      miniQMT broker
   (历史撮合)        (模拟撮合)       (实盘)
```

**关键边界**：
- 主体设计的 StrategyPackage v2 = Strategy Engine 的输入 spec
- Strategy Engine 不依赖 Qlib / vn.py，是 plain Python 的"决策大脑"
- 三个 adapter 都是薄壳（< 300 行），把 Engine 输出翻译为各自执行环境的 API
- vn.py 仅作为 OEMS 库（broker 接入 + 订单状态机 + 对账），**不嵌入主程序**——参考 `docs/architecture/paper_trading_v2_top_level_design.md` §8.2

## A.3 共享 Strategy Engine 层 — 推荐在主体设计基础上叠加

### A.3.1 核心动机

主体设计通过 Validation Modes A-F + Original Retest gate 保证"package promote 时与 QE 一致"，但**没有解决"package 已 promote 后，新策略子类（多 alpha 组合 / 公告信号合成 / 新执行算法）落地时三套实现漂移"问题**。

具体场景：用户后续要加入 `combination_rule = meta_learner` 这种新组合方式：
- 主体设计要求：通过 SOTA 流程晋级新 package + Validation Mode 验证
- **但每个新组合方式的代码实现仍要在 QE Qlib 端 + Paper v2 端 + 未来 Live 端各写一份** —— 三套实现迟早漂移

**Strategy Engine 层正是为了消除这个"实现漂移源"**：所有持仓决策代码集中在 Engine 内，三个 adapter 仅负责把 Engine 输出翻译给各自执行环境。

### A.3.2 Engine 内部职责清单

**Engine 必须实现**（plain Python，零外部依赖，可被三个 adapter 复用）：

| 模块 | 职责 | 来源 |
| --- | --- | --- |
| `score_to_candidates` | 全市场 score → topk + risk_policy + tradability 过滤 → candidates | 复用 `selection_center/risk_policy.py` + `selection_center/tradability.py` |
| `compute_weights` | candidates → weights（softmax / equal / rank / linear + min/max 约束） | 重构 `paper_trading_v2/strategy_package/runtime.py:602-664` `_compute_score_weighted_weights` |
| `apply_dynamic_ndrop` | 动态 n_drop 阈值（threshold_method / max_n_drop / min_n_drop） | 重构 `runtime.py:551-600` `_filter_dynamic_ndrop` |
| `apply_hold_thresh` | 持仓锁定期（不允许卖出未达 hold_thresh 天数的持仓） | 重构 `runtime.py:667-679` `_can_sell_under_hold_thresh` |
| `targets_to_intents` | 目标持仓 vs 当前持仓 → rebalance intent | 现有 `paper_trading_v2/day_runner.py` 部分逻辑迁入 |
| `compute_score_combination` | 多 alpha 时应用 `combination_rule`（weighted_sum / rank_aggregation / meta_learner） | 多 alpha 启动时新加（用户已确认推迟） |

**Engine 不实现**（留给 adapter 或外部）：
- 因子 / 模型推理 → 已共享于 `inference_engine.py`，Engine 接收 score 而非原始数据
- 撮合 / fill 模拟 → QE 用 Qlib，Paper / Live 用 vn.py
- 行情数据访问 → adapter 各自处理
- 订单状态机 / 对账 → vn.py OmsEngine（OEMS 层）

### A.3.3 七个交付物（与主体设计 Phase 编号衔接）

| # | 交付物 | 工作量 | 与主体 Phase 的依赖 |
| --- | --- | --- | --- |
| 1 | StrategySpec 接口定义（Pydantic 包装 StrategyPackage v2 + escape hatch `raw_extension` 字段） | **0.5 周**（直接 reuse 主体设计 schema） | 主体 Phase 5 完成 |
| 2 | Strategy Engine 核心实现（A.3.2 全部模块） | 2-3 周 | 主体 Phase 4 完成（确保 equivalence test 可复现） |
| 3 | QE Adapter | 2-3 周 | 主体 Phase 5 完成（Adapter 引用 model_registry schema） |
| 4 | Paper Adapter（trading_core RPC + SimGateway） | 1-2 周 | §16 vn.py MVP（独立工作流，不在本文档范围） |
| 5 | Live Adapter（trading_core RPC + vnpy_xt） | 1-2 周 | 实盘 PoC（独立工作流） |
| 6 | 现有代码迁移（`runtime.py` + `qe_strategies/topk_dropout_rc_qlib.py` 改造为调用 Engine） | 2-3 周 | #2 + #3 + #4 完成 |
| 7 | Equivalence 测试矩阵（与主体 Validation Modes A-F 整合） | 1-2 周 | #6 完成 |
| **合计** | | **9.5-15.5 周（可并行压缩到 5-7 周）** | |

### A.3.4 与主体 Validation Modes A-F 的整合

主体 Mode A-F 是"对单一 manifest 在不同场景下的 retest"，A.3 Engine 层带来一组新的等价性验证：**三个 adapter 对同一 manifest + 同一 score 输出的 OrderIntent 必须 100% 一致**。这条作为 Mode G（建议）：

```
Mode G: Cross-Adapter Equivalence Test
  Input:   StrategyPackage v2 manifest + 固定 score 数据 + 固定 current_positions
  Output:  List[OrderIntent] 来自 QE Adapter / Paper Adapter / Live Adapter
  Pass:    三组 OrderIntent 在 (symbol, direction, quantity) 维度 100% 一致
  Note:    NAV 差异由后续撮合层差异（Qlib vs vn.py）产生，不在 Engine 层 cover；
           Engine 层只保证决策一致
```

**Mode G 应作为 Engine 层 PR 合 main 的硬 gate**——失败则不合，否则三 adapter 漂移问题没消除。

### A.3.5 是否一定要做？决策权在用户

**不做的成本**（参考 `paper_v2_user_requirement_audit_20260507.md` §30.1）：每年 20-40 次类别 C 修改（演进 + 公告信号 + 滚动训练 trial）× 双工 +50-100% 时间 = **10-26 工作周/年的双工成本** + 漏改风险。

**做的一次性成本**：9.5-15.5 周（可并行压缩到 5-7 周）。

**第 1 年即回本**。但用户已决策"多 alpha 推迟"，这条紧迫性下降——可以接受 Codex 治理 + 双工 + retest gate 兜底，等 Class C 修改频率实际暴露后再决定。**建议作为 Tier 1B 内的可选工作流，与主体 Phase 5-6 并行设计，Phase 4 完成后再进入实施**。

## A.4 跨工具（Codex + Claude Code）协作规范

未来 Codex 主导主体设计 + Claude Code 主导 vn.py / Paper v2 集成，跨工具协作必须遵循下列约定。

### A.4.1 模块边界与分支命名

| 分支前缀 | 归属 | 工作面 |
| --- | --- | --- |
| `codex/<task>-<yyyymmdd>` | Codex | QE / Model Registry / Seed Contract / SOTA Hall / 主体 Phase 0-7 |
| `claude/<task>-<yyyymmdd>` | Claude Code | Paper v2 / trading_core / Strategy Engine / vn.py 集成 / UI |

**用户在每次任务分配时显式声明归属**——参见 `feedback_aistock_codex_alignment.md` 第 12 条。

### A.4.2 修改类型分类（A/B/C/D）与同步策略

参见 `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §24。简表：

| 类别 | 例子 | 同步策略 |
| --- | --- | --- |
| A 训练期修改 | seed / 训练超参 | **自动**（通过 model_artifact 传播，因模型权重 deterministic） |
| B 因子层修改 | 新因子定义 / 计算逻辑 | **自动**（已共享 inference_engine.py） |
| C 执行语义修改 | 持仓权重算法 / 动态 n_drop / 风险策略语义 / 多 alpha 组合 / 公告信号合成 | **如有 Strategy Engine 层（A.3）**：自动同步；**否则**：双工 + retest gate 兜底；PR 必须标 `[CROSS-STACK]` 并含双侧实现 |
| D 契约层修改 | manifest schema 加字段 / Frozen Alpha Core 扩展 / model_registry 加表 | **双 PR 模式**：先在产出端定义 schema v2 不启用 → 消费端加 v2 reader（兼容 v1）→ 都合后切默认 |

### A.4.3 类别 C 修改硬约束（无 Engine 层时）

如果未做 A.3 Engine 层，类别 C 修改 PR 必须满足：
1. PR 标签 `[CROSS-STACK]`
2. PR 描述显式列出"在 X 处和 Y 处都做了同等修改"
3. 至少 1 个 equivalence smoke test（同 manifest 同日，QE backtest vs Paper v2 重放，输出 diff 在容忍度内）
4. PR 合并前 cross-tester（另一方 agent）审核测试结果

**无 Engine 层时**——这是消除类别 C 漂移的最低保障；**有 Engine 层时**——结构上避免漂移，retest gate 仅作 last-line-of-defense。

### A.4.4 Schema 升级双 PR 模式

主体设计的 manifest / model_registry / strategy_pkg.* 等 schema 升级（类别 D 修改）必须按此模式：

```
PR 1（产出端）：在 QE / Codex 维护代码加 schema v2 字段定义
                schema v2 仅 optional / nullable，不启用 v2 默认产出
PR 2（消费端）：Paper v2 / Selection Center / Live Adapter 加 v2 reader
                兼容老 v1 manifest（默认值或迁移函数）
两个 PR 都合后 → PR 3：切 QE 默认产出 v2
```

**避免"产出端切 v2 但消费端读不了"的窗口期**。

### A.4.5 Cross-testing 约定

参见 `docs/analysis/paper_v2_user_requirement_audit_20260507.md` §20-§21：

- 分支前缀 `codex/*` → Cross-tester 是 Claude Code（执行 L2/L3 测试）
- 分支前缀 `claude/*` → Cross-tester 是 Codex
- **Cross-tester 只填 bug，不修代码**（权限隔离 + prompt + hook 三重保障）
- Bug 真源 = GitHub Issues + AIstock Validation Center
- 流水线现状（`backend/services/validation/` 4059 行已就位 + finding_store 已有 `assigned_agent` 字段）支持人工触发 cross-test，自动路由待补（约 1-2 周工作）

### A.4.6 共享必读文档清单

每个跨工具 PR 启动前，agent 必须读：

1. 本文档（含本附录）—— 治理 + Engine 层 + 协作规范
2. `docs/analysis/paper_v2_user_requirement_audit_20260507.md` —— 全方案推导背景
3. `docs/architecture/paper_trading_v2_top_level_design.md` §8 —— vn.py 参考要求
4. `docs/codex_project_memory.md` —— 生产端口 / 模块边界 / Git 提交规则
5. `feedback_aistock_codex_alignment.md` —— Claude Code 与 Codex 协调规则

## A.5 每 Phase 必备测试用例（设计阶段就加入）

主体设计未明确每 Phase 配套测试矩阵；本节补充。**关键原则**：**每个 Phase 的 PR 必须含 ① 实施代码 ② 对应测试矩阵 + 至少 3 个测试 case**——否则 PR 不合 main。

### A.5.1 主体 Phase 0-7 测试用例

| Phase | 必备测试用例 | L 等级 | 写入文件 |
| --- | --- | --- | --- |
| **Phase 0** 术语对齐 | 文档与代码术语一致性扫描（grep 旧术语 0 命中） | L0 | `tests/aistock_validation/modules/qe_governance.md`（**新建**） |
| **Phase 1** 手工 SOTA 流程 | UI "加入 SOTA 殿堂" 按钮 / 后端 API 创建 `REVIEW_PENDING` 记录 / 老的"自动 SOTA"不再发生 | L3 | 同上 |
| **Phase 2** Asset Freezing | promote 后资产复制到 protected 库 / sha256 写入 manifest / 老路径修改 manifest 应失败 | L2 | `tests/aistock_validation/modules/strategy_package_v2.md`（**新建**） |
| **Phase 3** Original-config Retest | 状态流转 `ASSET_FROZEN → ORIGINAL_RETESTING → ORIGINAL_RETEST_PASSED` / retest 失败包不能进 `PAPER_CANDIDATE` | L3 | 同上 |
| **Phase 4** Master Seed Contract（**核心 gate**） | **同 manifest 同 master_seed 跑两次：NAV 差异 < 0.01bp + 持仓 100% 相同**（不可妥协） / seed_fragility_score 计算正确性 | **L4 核心 gate** | `tests/aistock_validation/modules/qe_reproducibility.md`（**新建**） |
| **Phase 5** Model Library 4 层 | model_template / spec / trial / artifact 表 CRUD / 老 catalog → 新 registry 视图迁移正确 / lifecycle_event 替代删除 | L2 | `tests/aistock_validation/modules/model_registry.md`（**新建**） |
| **Phase 6** Runtime Variants | variant 独立 hash / 不能修改 frozen core / validation 通过才进 `PAPER_CANDIDATE` | L3 | `tests/aistock_validation/modules/strategy_package_v2.md` |
| **Phase 7** Latest-data + Rolling Validation | Mode A-F 各 mode 跑通 / rolling-train 模式产出新 ModelArtifact 进 `ORIGINAL_RETESTING` | L4 | `tests/aistock_validation/modules/qe_validation_modes.md`（**新建**） |

**新建测试矩阵文件 5 个**——直接把当前模块测试覆盖度从 7/30+ 推到 12/30+。

### A.5.2 Phase 4（Master Seed Contract）核心 gate 详细要求

这是整个主体设计中最关键的 L4 验收点：

```
Test name: qe_reproducibility_l4_seed_contract_strict
Trigger:   Phase 4 PR 合 main 前
Setup:     选 1 个有代表性的 ST PIT manifest（覆盖 LGB / NN 至少各一）
Steps:
  1. 用相同 master_seed 提交两次完整 QE 训练 + 回测
  2. 对比两次输出的 NAV 序列、持仓序列、订单序列
Pass:
  - NAV 每日差异绝对值 < 0.01bp（不是平均，是逐日 max）
  - 持仓集合 100% 相同（每个交易日的持仓股票 + 数量都相同）
  - 订单序列 100% 相同（symbol + direction + quantity + 触发日期）
Fail handling:
  - 如有任何差异，定位差异源（哪个子 seed 没固定 / 哪个库版本不一致 / 哪个 GPU 非确定性）
  - 修复后重跑直至通过
  - 不通过则 Phase 4 PR 不能合 main
```

### A.5.3 与 Cross-testing 衔接

参见 §A.4.5。Codex 实施 Phase 0-7 时：
- Codex 写每 Phase 的实施代码 + 测试矩阵 + 测试 case
- **Claude Code 作为 cross-tester 执行测试**（不是 Codex 自测）—— 实现"开发与测试不同人"的 cross-test 价值
- 发现的 bug 走 Validation Center / GitHub Issues 流程

## A.6 未来架构演进的约定

### A.6.1 新增 alpha 类型（多 alpha 推迟后启动时）

按用户决策，多 alpha 推迟到单 alpha 探索方向成熟后启动。届时接入约定：

**有 Strategy Engine 层（A.3 已实施）**：
- 在 StrategyPackage v2 manifest 加 `combination_rule: weighted_sum / rank_aggregation / meta_learner`
- Strategy Engine 加 `compute_score_combination()` 处理新 rule
- 三个 adapter 不变，自动获得多 alpha 能力
- 改动行数 ~200-400，**仅在共享层**

**无 Engine 层**：
- QE Qlib 端 / Paper v2 / Live Adapter 各实现一份
- 改动 ~600-1200 行 + 三套 retest
- 漂移风险存在，靠 Cross-Adapter Equivalence Test（Mode G 等价物）持续监控

### A.6.2 公告 / 财报独立信号接入（用户路线图 #5）

类似多 alpha：

**有 Engine 层**：
- StrategyPackage v2 的 `alpha_components` 接受新类型 `EventSignalComponent`
- Engine 在 `compute_scores()` 阶段把事件信号合入 score 计算
- 三 adapter 自动支持

**无 Engine 层**：QE Qlib YAML 端 + Paper inference 端各写一次合成代码。

### A.6.3 Paper v2 → 实盘切换

**有 Engine 层 + 完整 trading_core**：
```
Paper portfolio 的 trading_core 配置：
  broker_adapter: SimGateway → vnpy_xt
启动后：Strategy Engine 不变 / StrategySpec 不变 / 唯一变化：trading_core 内部 broker
```
**配置项变更，不是代码变更**。

**无 Engine 层**：实盘相当于第三套独立策略实现，3-4 周额外工作。

### A.6.4 滚动训练接入（主体 Phase 7 + 后续滚动训练专项）

主体 Phase 7 已经设计 latest-data + rolling-train validation。配套的"自动晋级新 ModelArtifact"工作流：

```
Trainer Scheduler（按周/月触发）→ 训练新 ModelTrial → 产出新 ModelArtifact
   → 自动 Mode A / Mode F validation
   → 通过则进 ORIGINAL_RETESTING
   → 通过 retest 则更新 StrategyPackage v2 的 model_artifact_pointer
   → 三个 adapter（如有）自动用上新权重
   → Equivalence Mode G 验证三端输出一致
```

**Strategy Engine 层在此场景下价值显著**——新权重无须改任何代码，仅 manifest pointer 变更。

### A.6.5 Frozen Alpha Core 之外的 spec 字段需要 escape hatch

主体 Frozen Alpha Core 设计了完整的字段集，但**未来扩展时不可避免会发现新场景**（如新损失函数、新 ensemble 方式、新前置过滤规则）。建议在 StrategyPackage v2 manifest 加：

```yaml
custom_extension:
  type: object
  description: |
    Reserved for future spec fields not yet formalized into Frozen Alpha Core.
    Each entry should have a clear field name + version tag, and ideally be
    promoted to first-class field in next Frozen Alpha Core version.
  example:
    new_feature_v1: { ... }
```

**避免每次小扩展都触发完整 schema 升级 + 双 PR 流程**。

## A.7 立即启动的优先级工作（Week 1-2）

按 `paper_v2_user_requirement_audit_20260507.md` §31.5：

| # | 工作 | 归属 | 工作量 | 启动时机 |
| --- | --- | --- | --- | --- |
| 1 | **Phase 4 Master Seed Contract**（最高优先级） | **Codex** | 2-3 周 | **立即** |
| 2 | **Phase 0-1 治理流程基础**（术语对齐 + 手工 SOTA 按钮） | **Codex** | 1-2 周 | 立即（与 #1 并行） |
| 3 | **vn.py + miniQMT PoC**（3-5 天连通性） | **Claude Code** | 3-5 天 | **立即** |
| 4 | **vn.py + Paper v2 集成 MVP** | **Claude Code 多窗口** | 4 周 | PoC 通过后立即 |
| 5 | **Strategy Engine 接口纸面设计**（A.3 #1） | Claude Code | 0.5-1 周 | Week 2 起 |
| 6 | **ST PIT spans 数据补到最新交易日** | Codex | 0.5-1 周 | 立即（与 #1 并行） |
| 7 | **Phase 5 Model Library**（4 层架构） | Codex | 3-4 周 | Week 2-3 启动 |

**Week 1 末预期状态**：
- vn.py PoC 完成（go/no-go 信号）
- Codex Phase 0-1 完成（UI 上能看到"加入 SOTA 殿堂"按钮）
- Codex Phase 4 进展过半（seed contract schema 落地）
- ST PIT 数据补完最新交易日

**关键执行约束**：**Phase 4 (Master Seed Contract) 是后续所有工作的解锁条件**——2-3 周内由 Codex 完成是 Tier 0+/Tier 1B 之间的硬时序分界。

## A.8 用户必须拍板的关键决策

下列三项决定后续路径，建议尽早确认：

1. **是否在主体设计基础上叠加 A.3 共享 Strategy Engine 层？**
   - 选 A：仅主体设计 + 类别 C 双工 + retest gate 兜底（多 alpha 推迟下可接受）
   - 选 B：主体设计 + A.3 Engine 层（一次投入避免长期类别 C 漂移）
   - 推荐：在多 alpha 推迟下**先选 A，观察 Class C 修改实际频率 1-2 个月再决定是否升级到选 B**——避免在 Tier 0+ 期间分散精力
2. **每 Phase PR 必须含测试矩阵的硬约束是否启用？**
   - 推荐：是（不可妥协）—— 否则覆盖率永远在 20% 徘徊
3. **Phase 4 的"两次跑 NAV 差异 < 0.01bp"是否作为 L4 核心 gate 不可妥协？**
   - 推荐：是—— 否则 Master Seed Contract 价值减半

## A.9 引用与依据

- 本文档主体（第 1-16 章）= Codex 治理设计
- `docs/analysis/paper_v2_user_requirement_audit_20260507.md` = Claude Code 完整审计与方案推导（含 §29-§32 详细引用）
- `docs/architecture/paper_trading_v2_top_level_design.md` §8 = vn.py 参考要求（设计文档原文）
- `docs/codex_project_memory.md` = Codex 维护规则（生产 8001 端口约束 / 模块边界 / Git 提交要求）
- `feedback_aistock_codex_alignment.md`（用户级）= Claude Code 与 Codex 协调规则（13 条约定）
- `tests/aistock_validation/catalog/test_levels.md` = L0-L5 验证等级定义
- `tests/aistock_validation/catalog/module_registry.yaml` = 当前模块注册表（30+ 模块）

---

**附录到此结束**。如对附录任一节有异议，建议在用户协调下双方讨论修订；对主体设计（第 1-16 章）的修改属 Codex 维护范围，本附录不直接修改。

> **重复一次本附录开头的免责声明**：附录内容以补充建议形式存在，与主体（第 1-16 章）冲突时以主体为准。Codex 可在主体设计实施过程中评估附录建议的采纳与否；用户最终拍板。

---

# 附录 B：实施策略 —— 分支与数据库隔离

> **作者**：Claude Code（Opus 4.7）
> **生成时间**：2026-05-08
> **来源参考**：`docs/analysis/paper_v2_user_requirement_audit_20260507.md` §33-§34
> **状态**：本附录为实施操作建议，**不修改本文档第 1-16 章主体内容**。
>
> **附录 B 目的**：保证 Codex Phase 0-7 实施期间 main 分支的生产稳定性（8001 端口 / 现有 QE 实验 / Selection / Paper v2 现有功能不受影响），同时给出资源受限场景下的可行路径。

## B.1 长期 feature 分支策略

### B.1.1 推荐分支结构

```
main（生产稳定，实验持续运行，UI 8001 端口跑当前代码）
 │
 ├─ codex/qe-governance-integration-20260508（长期集成分支，Codex 主导）
 │   ├─ codex/qe-phase-0-terminology-20260508       (Phase 0 worktree)
 │   ├─ codex/qe-phase-1-manual-sota-flow-20260508  (Phase 1)
 │   ├─ codex/qe-phase-4-seed-contract-20260508     (Phase 4，最高优先级)
 │   ├─ codex/qe-phase-5-model-library-20260510     (Phase 5)
 │   ├─ codex/qe-phase-2-asset-freezing-20260520    (Phase 2)
 │   ├─ codex/qe-phase-3-original-retest-20260601   (Phase 3)
 │   ├─ codex/qe-phase-6-runtime-variants-20260615  (Phase 6)
 │   └─ codex/qe-phase-7-rolling-validation-20260701 (Phase 7)
 │
 ├─ claude/paper-v2-vnpy-mvp-20260508（Claude Code，独立于 Codex 集成分支）
 └─ claude/strategy-engine-design-20260508（§A.3 §25 B+ 纸面 + 后续实施）
```

### B.1.2 工作流

```
1. Codex 在 codex/qe-phase-X-yyyymmdd 完成 Phase X 实施
   ├── 写代码 + 测试矩阵（附录 A.5 强制要求）
   ├── L0 守护扫描通过
   └── L1 单测通过

2. Codex 提 PR：codex/qe-phase-X → codex/qe-governance-integration-20260508
   ├── Cross-tester（Claude Code）在集成分支跑 L2/L3
   ├── bug 进 GitHub Issues + Validation Center
   └── 合入集成分支

3. 集成分支定期跑全套 L4 验证（含 Mode A-F + 附录 A.5.2 Phase 4 核心 gate）

4. 全部 Phase 完成 → 走 B.4 merge gate → 合 main → 用户授权重启 8001
```

### B.1.3 main 上的工作完全不受影响

| 项目 | main 上的状态 | 集成分支期间影响 |
| --- | --- | --- |
| 生产 FastAPI 8001 | 跑当前 main 代码 | **零影响**——8001 不重启 |
| 已运行的 QE 实验 | 跑当前 manifest schema v1 | 零影响 |
| RD-Agent worker | WSL 上跑当前代码 | 零影响 |
| Selection Center / Paper v2 现有功能 | main 代码可用 | 零影响 |
| ST PIT 数据补齐 | 直接在 main 推进 | **正常推进**——是数据修复不动 schema |
| 用户 UI 操作 | 跑 main 代码 | 零影响 |

## B.2 与 Claude Code 并行工作的协调

| Claude Code 工作流 | 是否依赖 Codex 集成分支 | 处理 |
| --- | --- | --- |
| `claude/paper-v2-vnpy-mvp-20260508`（vn.py PoC + MVP） | **不依赖**——用 main 现有 ST PIT manifest 跑 demo | Claude Code 直接在 main 基础上开分支 |
| `claude/strategy-engine-design-20260508`（§A.3 纸面） | 不依赖（仅参考本文档） | 同上 |
| §A.3 Engine 实施 | **依赖** Codex Phase 4 完成 | 等 Phase 4 合入集成分支后启动 |
| §A.3 QE Adapter 实施 | **依赖** Codex Phase 5 完成 | 等 Phase 5 合入集成分支后启动 |

**协调约定**：
1. Claude Code **不主动合入 Codex 集成分支**——避免污染
2. §A.3 Engine 需 Codex schema 时 Claude Code 单独 fetch 集成分支读取，但代码仍提交到 `claude/*`
3. **最终合 main 时**：Codex 集成分支先合 → Claude Code 的 §A.3 分支再合（**不能同时**）
4. 双方分支每 2 周 rebase main 一次保持同步

## B.3 数据库隔离 —— 两种方案

### B.3.1 方案 A：dev DB（资源充足时）

新建 `aistock_dev` 独立数据库，集成分支代码连 dev DB。生产 DB 完全不动。

**优势**：物理隔离、几乎零风险、误操作可重建。
**代价**：+10-50 GB 磁盘占用 + 数据复制成本。

### B.3.2 方案 B：生产 DB + 严格 additive only（资源受限时，**推荐**）

直接用生产 DB，但所有 schema 变更只增不改不删。这是业界标准做法（expand-contract pattern，Stripe / Shopify / GitHub 等都用）。

**节省的资源**：~10-50 GB（无需复制 DB）。
**风险等级**：低（前提是 6 条硬规则严格执行——见 B.5）。

## B.4 集成分支 → main 的合入条件（merge gate）

集成分支只在满足下列**全部条件**时才允许合 main：

| # | 条件 | 验收方式 |
| --- | --- | --- |
| 1 | Phase 0-7 全部完成（含测试矩阵） | 附录 A.5 全表 ✓ |
| 2 | **Phase 4 L4 核心 gate 通过**（同 manifest 同 master_seed 两次跑 NAV 差异 < 0.01bp + 持仓 100% 相同） | `tests/aistock_validation/history/qe_reproducibility/...l4_seed_contract_strict.json` |
| 3 | 集成分支跑完整 L4 通过 | `nox -s aistock_validation_l4` 通过 |
| 4 | 现有 main 上的 4 个 LEGACY_NON_ST_PIT 包迁移决策已落实 | Phase 1 内决定 |
| 5 | 老 manifest schema v1 兼容性测试通过 | Phase 5 测试矩阵覆盖 |
| 6 | 生产 DB 迁移脚本写好 + 在 dev 环境 dry-run 通过 + 有回滚 SQL | Phase 5 交付物 |
| 7 | 用户最终签字（含合并时间窗 + 8001 重启时间窗） | 用户当回合明确确认 |

**任一条不满足，集成分支不合 main**——金融 IT 硬性规则。

## B.5 方案 B（生产 DB additive only）的 6 条硬规则

| # | 规则 | 例子 |
| --- | --- | --- |
| 1 | **新表必须独立 schema 命名空间** | `model_registry.model_template` 等放 `model_registry` schema；`strategy_pkg.promotion_review` 放 `strategy_pkg` schema —— **不能新表落到 `public` schema** |
| 2 | **新字段必须 NULL 或有 DEFAULT** | `ALTER TABLE strategy_pkg.package ADD COLUMN seed_policy TEXT NULL` 或 `... DEFAULT 'unset_legacy' NOT NULL` —— **不能加 NOT NULL 无 DEFAULT** |
| 3 | **不修改 / 不删除任何现有字段或表** | 即使发现旧字段命名不好或类型不对，也只能"新增更好的字段 + 双写 + 后续合 main 时切 reader" |
| 4 | **新表不引用现有表带 CASCADE 删除** | `FOREIGN KEY ... ON DELETE CASCADE` 禁用——防止意外连锁删除 |
| 5 | **集成分支只创建新记录，不修改现有记录** | 测试用**新 manifest_id / package_id**（dev/test 前缀）；现有 4 个 LEGACY_NON_ST_PIT 包**只读不改** |
| 6 | **写入新字段的查询必须显式判空** | 老代码 SELECT * 仍可工作（新字段 NULL 不影响）；新代码读新字段时 IS NULL → 走 fallback |

**任一条破例都会破坏隔离**。

### B.5.1 各 Phase 的 additive 落地示例

#### Phase 4 Master Seed Contract

```sql
-- 加可空字段到现有 strategy_pkg.package 表
ALTER TABLE strategy_pkg.package ADD COLUMN seed_policy TEXT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN master_seed BIGINT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN seed_sequence JSONB NULL;
-- (其他子 seed 字段同理)

-- 新建独立审计表
CREATE TABLE strategy_pkg.seed_fragility_score (
    package_id TEXT PRIMARY KEY REFERENCES strategy_pkg.package(package_id),
    metric_mean_by_seed JSONB,
    seed_sensitivity_score DOUBLE PRECISION,
    -- (per 主体第 4 章 setup)
    created_at TIMESTAMPTZ DEFAULT now()
);
```

**生产侧零影响**：老代码不知新字段；老 manifest 新字段 NULL（视为 `unset_legacy`）；`seed_fragility_score` 是新表。

#### Phase 5 Model Library 4 层

```sql
CREATE SCHEMA IF NOT EXISTS model_registry;

CREATE TABLE model_registry.model_template (...);
CREATE TABLE model_registry.model_spec (...);
CREATE TABLE model_registry.model_trial (...);
CREATE TABLE model_registry.model_artifact (...);
CREATE TABLE model_registry.model_lifecycle_event (...);
```

**生产侧零影响**：`aistock_model_catalog` 完全不动；新 schema 独立；合 main 后 `aistock_model_catalog` 保留作兼容视图（per 主体 line 828-868）。

#### Phase 1-2 / 6-7

新增表全部在 `strategy_pkg` schema 下扩展（`promotion_review` / `package_validation_run` / `package_runtime_variant`），**不动 `strategy_pkg.package` 现有字段**。

### B.5.2 必须避免的破规则做法

| 破规则做法 | 后果 |
| --- | --- |
| `ALTER TABLE ... DROP COLUMN ...` | 生产代码崩溃 |
| `ALTER TABLE ... ALTER COLUMN ... TYPE ...` | 生产代码读到不期望类型 |
| 给老表加 NOT NULL 无 DEFAULT 字段 | 老 INSERT 缺字段直接报错 |
| 删除现有索引 | 生产查询性能塌方 |
| 集成分支修改现有 package 记录 | 与 main 实验冲突 |
| 现有表加 CASCADE 外键 | 删除连锁 |
| 集成分支跑测试时复用生产 ID | 与生产并发写冲突 |

### B.5.3 集成分支测试的"新 ID 隔离"策略

为避免与生产实验冲突，集成分支测试**必须用专属 ID 命名空间**：

| 资源 | 生产命名 | 集成分支命名 |
| --- | --- | --- |
| package_id | `pkg_xxx` | `pkg_dev_xxx` 或 `pkg_test_yyyy` |
| manifest_id | `mfst_xxx` | `mfst_dev_xxx` |
| qe_task_id | `qe_yyyymmdd_xxxx` | `qe_dev_yyyymmdd_xxxx` |
| protected asset 路径 | `rdagent_assets/strategy_package_runtime/<sha>/` | `rdagent_assets/strategy_package_runtime_dev/<sha>/` |

**集成分支代码读时**：可以查所有 ID（含生产）；
**写时**：只允许写 dev/test 前缀的 ID。

### B.5.4 PR 合入集成分支前的 Cross-test 检查清单

每次 Codex Phase X PR 合入集成分支前，Cross-tester（Claude Code）按下列清单核对：

- [ ] 没有 `ALTER TABLE ... DROP COLUMN`
- [ ] 没有 `ALTER COLUMN TYPE`
- [ ] 新字段都 NULL 或有 DEFAULT
- [ ] 新表都在独立 schema（`model_registry` / `strategy_pkg` 等），不在 `public`
- [ ] 没有引入 CASCADE 外键到现有表
- [ ] 测试用 dev/test 前缀 ID
- [ ] 不修改现有记录字段值（除 Phase 1 一次性的 LEGACY 标记 + 必须用户授权）

任一条不符合 → PR 不合 → 修复 → 重审。

## B.6 工作区资产隔离

Phase 2 Asset Freezing 涉及向 protected 库复制资产。生产路径与集成分支路径必须分开：

```
生产路径: F:\Dev\AIstock\rdagent_assets\strategy_package_runtime\          ← main 在用
集成路径: F:\Dev\AIstock\rdagent_assets\strategy_package_runtime_dev\      ← 集成分支用
         或
         F:\Dev\AIstock_worktrees\qe-governance-integration-*\rdagent_assets\...
```

### B.6.1 ST PIT 数据等只读数据可共享

ST PIT spans / suspend_d / stk_limit / 行情数据等**只读数据**集成分支可与 main 共享——这些不会被开发过程破坏。Phase 4-5 不修改这些表。

## B.7 现有数据 / manifest 的迁移计划

合 main 后，main 上现有的 4 个 LEGACY_NON_ST_PIT manifest 需要迁移：

| Manifest | 当前状态 | 迁移目标 | 时机 |
| --- | --- | --- | --- |
| 4 个 LEGACY_NON_ST_PIT | manifest schema v1 + ST PIT 合约缺失 | 标记 `lifecycle_status=LEGACY` + `seed_policy=unset_legacy` + `protected_asset=true` | Phase 1 末（合 main 时） |
| 已有 selection artifact | v1 格式 | 保留作诊断；不晋级到新流程 | Phase 1 末 |
| 历史 QE 实验记录 | 现有 schema | 保留只读；新实验产出 v2 schema | 自然演进 |

**迁移本身是 additive**——老数据保留、新流程不读老数据；不存在"全库替换"风险。

## B.8 回滚计划

虽然有 B.4 的 7 条 merge gate，仍要准备回滚：

| 故障类型 | 回滚动作 |
| --- | --- |
| 生产 8001 启动失败 | `git revert <merge-commit>` + 重启 8001 + 监控 |
| 现有 manifest 在新代码下读取失败 | 同上 + 紧急修 v1 reader 兼容性 |
| 新增 model_registry 表 schema 错误 | DB 迁移 down script + 代码 revert |
| 演进 / 实验跑不通 | revert + 修 + 重新走 merge gate |

**revert 要快**——金融 IT 不允许"先观察一下"。出问题立即 revert，再分析根因。

## B.9 磁盘空间预估（6 个月 Phase 0-7 累计）

### B.9.1 数据库新增（采用方案 B additive only）

| 新表 | 预估行数 | 预估大小 |
| --- | --- | --- |
| `model_registry.model_template` | 5-20 | < 1 MB |
| `model_registry.model_spec` | 50-200 | < 10 MB |
| `model_registry.model_trial` | 5000-20000 | 100-500 MB |
| `model_registry.model_artifact`（仅元数据） | 5000-20000 | 50-200 MB |
| `model_registry.model_lifecycle_event` | 10000-50000 | 50-200 MB |
| `strategy_pkg.promotion_review` | 50-200 | < 10 MB |
| `strategy_pkg.package_validation_run` | 500-2000 | 50-200 MB |
| `strategy_pkg.package_runtime_variant` | 100-500 | 10-50 MB |
| `strategy_pkg.seed_fragility_score` | 50-200 | < 10 MB |
| **DB 总计新增** | | **~300 MB - 1.2 GB** |

### B.9.2 文件系统（模型权重）

**这才是大头**：

| 类别 | 单个大小 | 6 个月累计 |
| --- | ---: | ---: |
| LGB 模型权重 | 10-50 MB | 50-100 GB |
| NN 模型权重（PyTorch / TensorFlow） | 100-500 MB | 200-500 GB |
| 因子衍生中间产物 | 几 MB-几十 MB | 5-20 GB |
| Validation Mode A-F 输出（NAV / 持仓 / pred.pkl） | 几 MB | 5-20 GB |
| **文件系统总计新增** | | **50-500 GB** |

**操作建议**：
- 检查 `F:\` 当前剩余空间 + `rdagent_assets/` 当前占用
- 如剩余 < 200 GB，应用旧权重清理策略（per 主体 line 220 `RETIRED` 终态可归档冷存储）
- 考虑给 `rdagent_assets/` 单独挂载更大磁盘（如果 `F:\` 紧张）

## B.10 Day 1 推荐启动步骤

```
Day 1 (用户 + Codex):
  1. 检查 F: 磁盘剩余空间 + rdagent_assets/ 当前占用
  2. 用户授权 Codex 创建集成分支:
     git checkout -b codex/qe-governance-integration-20260508
     git push -u origin codex/qe-governance-integration-20260508
  3. 用户在 GitHub 设置分支保护（PR required + L0/L1 必过）
  4. （可选）用户决定 dev DB 命名（aistock_dev）+ 创建
       如果磁盘紧张直接走 B.5 方案 B（不创 dev DB）

Day 1 (Codex):
  5. Codex 启动 worktree: codex/qe-phase-4-seed-contract-20260508
  6. Codex 同时启动 worktree: codex/qe-phase-0-terminology-20260508
  7. Codex 同时启动 worktree: codex/qe-phase-1-manual-sota-flow-20260508

Day 1 (Claude Code，独立):
  8. Claude Code 启动 worktree: claude/paper-v2-vnpy-mvp-20260508
  9. 第一步: vn.py + miniQMT PoC（3-5 天）

Week 1-2:
  Codex Phase 0-1-4 推进; 合入集成分支后 Claude Code 跑 cross-test
  Claude Code vn.py PoC + MVP 启动

Week 3-6:
  Codex Phase 5 启动; Phase 4 完成
  Claude Code §A.3 Engine 设计（纸面）
  Claude Code vn.py MVP 主体推进

Week 7-10:
  Codex Phase 2-3 + 6-7 推进
  Claude Code vn.py MVP 完成 + §A.3 实施

Week 11-15:
  集成分支跑完整 L4 + Mode A-F
  B.4 merge gate 7 条逐一验证
  通过后合 main + 用户授权重启 8001

Week 16+:
  正式启动 #5 公告信号 / 滚动训练 / 实盘准备（基于稳定的新 main）
```

## B.11 一句话结论

**Codex Phase 0-7 在长期集成分支 `codex/qe-governance-integration-20260508` 上开发是工程标准做法、强烈推荐**——main 上的 8001 / 实验 / 用户操作完全不受影响。

**资源受限场景下**，使用方案 B（生产 DB additive only + 6 条硬规则）替代方案 A（dev DB）—— 节省 10-50 GB 数据库空间，风险接近（前提规则严格执行）。

**真正要担心的是文件系统空间（模型权重 50-500 GB）**——而非 DB 空间（~ 1 GB）。建议先检查 `F:\` 剩余空间 + 准备 RETIRED 模型归档策略。

**集成分支 → main 的 7 条 merge gate**（B.4）+ **方案 B 的 6 条硬规则**（B.5）+ **PR 合并前的 Cross-test 检查清单**（B.5.4）—— 三层保护一起执行，可保证生产稳定性。

**Claude Code vn.py MVP 工作独立**——不污染 Codex 集成分支；最终合 main 时双方分支序列合入，不能同时。

---

**附录 B 到此结束**。如对附录任一节有异议，建议在用户协调下双方讨论修订。

> **重申免责声明**：附录内容以补充建议形式存在，与主体（第 1-16 章）冲突时以主体为准。Codex 可在主体设计实施过程中评估附录建议的采纳与否；用户最终拍板。

---

## Appendix C: QE/HMM Hotfix Cross Reference (2026-05-08)

The implementation of this governance design must also include the two QE/HMM remediation items defined in `docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md`:

1. **backtest-only recorder isolation**: this is a P0 bugfix. Before any original-config retest, runtime-variant retest, or backtest-only loop can become Paper-ready, the target recorder must be loop-local, non-symlinked, and not the same realpath as the source `mlruns`.
2. **capacity-parameterized ScoreWeighted V2 strategy asset**: do not directly change the legacy `score_weighted_topk_v2` behavior. Add a new versioned strategy file and strategy_id, make it selectable in DB/UI, and keep the legacy strategy labeled as `legacy_5m_cap` or an equivalent capacity-constrained profile.

These remediation items do not change the main responsibility split: QE discovers combinations, SOTA Hall handles manual review and promotion, StrategyPackage remains the single standard asset for Selection/Paper/future live candidates, the model registry stores model specs/trials/artifacts/seeds/history, and the warehouse stores durable analytical facts. They add required infrastructure correctness and strategy-versioning gates before an experiment result can move through that governance flow.
