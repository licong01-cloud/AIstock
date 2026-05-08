# QE SOTA 殿堂、StrategyPackage 资产治理、Seed 可复现性与模型库设计

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
