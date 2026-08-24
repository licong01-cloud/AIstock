# Advisory P0-F SHORT_REBOUND Policy Utility Ranker F2 详细设计

> 日期：2026-08-24
> Tier：F2
> 状态：`DESIGN_READY_IMPLEMENTATION_NOT_STARTED`
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置实验：P0-D binary meta-label、P0-E outcome-weighted binary meta-label
> 本文档阶段：只冻结下一代模型实验和条件运行时范围，不代表模型训练、源码实现、激活或生产验收已完成
> 2026-08-24 停牌覆盖修订：feature schema、reference parity、advancement 和 Stage B confidence 条款由 `docs/architecture/advisory_p0f_suspension_aware_feature_parity_f2_revision_20260824.md` 取代；其余条款继续有效。

## 1. Background / 背景与失败归因

P0-D 在冻结 28-path CPCV 中相对 matched Selection Top5 提高 `3.6556 bps`，path win rate 为 `64.29%`，但已消费的24决策日历史虚拟前向显示：命中率提高 `9.74pp`，累计净收益却低 `2.54pp`、最大回撤差 `5.63pp`、换手高 `5.33pp`。逐 episode 归因进一步显示：

- P0-D `take_probability` 与 candidate `net_excess_return_bps` 的 Spearman 相关性仅约 `0.10`。
- P0-D 22/30 次 entry 来自 Selection rank > 5，平均 episode 收益约 `-411.16 bps`。
- P0-D 平均盈利约 `+349.17 bps`、平均亏损约 `-758.87 bps`，主要损失来自错过大盈利和错误提升候选，而不是交易成本差异。

P0-E 以收益绝对幅度作为 binary loss sample weight，在同一已消费窗口改善了累计收益、回撤和换手，但冻结 CPCV 相对 P0-D 低 `0.473055 bps`、只有 `35.71%` path 胜出，PBO 从 `0.40` 恶化到 `0.8143`。这证明“让二分类更重视大幅度样本”不足以得到可泛化收益排序。

P0-F 因此只检验一个新的因果假设：**不再间接优化 take/skip，而是直接预测冻结 shadow policy 下的连续候选净超额收益，并按预测 policy utility 排序 Selection Top20。**

## 2. Scope / 范围

### 2.1 本设计冻结的能力

1. 复用 exact P0-C policy dataset、PIT feature schema、8 blocks 和 28 READY CPCV paths。
2. 使用 `MATURED net_excess_return_bps` 作为唯一监督目标；label 仍由冻结 Top5 `model_shadow_review_policy` 的候选反事实 episode 产生。
3. 在每条 path 内只用 train rows 拟合 robust location/scale，对连续收益执行可逆仿射标准化。
4. 训练固定 LightGBM Huber regression 的 `CORE` 和 `CORE_HMM` 两个 family、三个既有 seed，不增加开放搜索。
5. validation 每日按预测 `policy_net_excess_return_bps` 降序生成 exact Selection Top20 的 `entry_priority_rank`；退出仍使用 Selection Top40 rank。
6. 使用现有 shared shadow portfolio policy 评价真实组合净超额收益、回撤、换手和 episode；候选 regression loss 只作诊断。
7. 与 exact P0-D winner 做28-path配对比较；P0-E、Selection Top5、HMM、随机和 Candidate20 作为固定对照。
8. 只有预注册的 CPCV advancement contract 全部通过，才进入条件运行时/历史回放阶段；否则以负面离线实验终止。
9. 自然 future OOS 继续作为最终独立证据；任何已消费窗口只可标记为 `HISTORICAL_REPLAY`。

### 2.2 允许的未来实现文件范围

Stage A 离线研究固定为：

```text
backend/services/advisory_model_first/policy_utility_contracts.py
backend/services/advisory_model_first/policy_utility_training.py
backend/services/advisory_model_first/policy_utility_pipeline.py
backend/services/advisory_model_first/policy_utility_bundle.py
scripts/advisory_policy_utility_prepare_request.py
scripts/wsl/advisory_policy_utility_train.py
backend/tests/advisory_model_first/test_policy_utility_contracts.py
backend/tests/advisory_model_first/test_policy_utility_training.py
backend/tests/advisory_model_first/test_policy_utility_pipeline.py
backend/tests/advisory_model_first/test_policy_utility_bundle.py
```

只有 Stage A advancement contract 通过后，Stage B 才允许增加：

```text
backend/services/advisory_model_first/model_binding_resolution.py
backend/services/advisory_model_first/model_inference.py
backend/services/advisory_model_first/historical_forward_replay.py
scripts/advisory_p0d_historical_forward_replay.py
backend/tests/advisory_model_first/test_dynamic_model_binding.py
backend/tests/advisory_model_first/test_policy_utility_runtime_inference.py
backend/tests/advisory_model_first/test_historical_forward_replay.py
```

Stage B 是结果条件范围，不是 Stage A 完成的隐含要求。Stage A 负面时停止而不开发运行时，不属于简化或 partial 交付。

## 3. Non-goals / 非目标

- 不修改 P0-C labels、Selection Top20/Top40、父策略包排序、shadow/cost policy、target count、daily replacement budget、止盈止损、trailing protection、rank exit 或 time stop。
- 不使用旧24决策日回放结果、自然 forward observation 或未成熟 episode 选择 objective、family、seed、特征、阈值或停止轮数。
- 不训练 LambdaRank relevance、神经网络、双回归 head、quantile grid、ensemble、blend 或第三个 family。
- 不对收益 label clipping、winsorization、sample weighting、候选 rank guard 和 TopN 截断做结果后搜索。
- 不把 PBO 变成人工审批门禁；它保持选择偏差诊断。
- 不在 Stage A 修改 API/UI、数据库、scheduler、生产 descriptor、active binding、Paper、Simulation、QMT 或 Selection writes。
- 不新增证据归档、通用 ModelOps、缓存平台、调度平台、角色审批或历史补账。
- 本设计任务不训练模型、不发布 artifact、不提交/推送/合入、不重启后端、不执行 DDL/DML、不激活模型。

## 4. Architecture / 架构

### 4.1 Stage A：离线 policy utility 训练

```text
exact P0-C bundle + existing feature schema + 28 READY CPCV paths
  -> path-local train / validation rows
  -> train-only median location and MAD scale
  -> reversible standardized continuous policy return target
  -> fixed LightGBM Huber regression (CORE / CORE_HMM × 3 seeds)
  -> inverse-transform predicted_policy_net_excess_return_bps
  -> deterministic daily rank over exact Selection Top20
  -> existing shared Top5 shadow portfolio evaluation
  -> 168 trial-path rows + PBO + candidate regression diagnostics
  -> exact paired comparison with current P0-D
  -> immutable offline experimental bundle
  -> advancement contract PASS or NEGATIVE_STOP
```

### 4.2 Stage B：条件运行时与历史回放

```text
Stage A advancement PASS
  -> self-contained utility model + exact P0-D confidence model identity
  -> new explicit model_role
  -> backward-compatible inference payload
  -> isolated descriptor only
  -> previously consumed window as HISTORICAL_REPLAY diagnostic
  -> natural future OOS as independent confirmation
```

Stage B 不复用隐式“latest bundle”扫描。utility model、P0-D confidence model、feature schema、P0-C dataset、policy、cost、package、Program/binding 和 source commit 均进入 request/bundle/descriptor identity。

## 5. Contracts / 契约

### 5.1 Frozen request v1

新 request 类型为 `FrozenAdvisoryPolicyUtilityTrainingRequestV1`：

```text
schema_version = frozen_advisory_policy_utility_training_request_v1
training_objective = HUBER_CONTINUOUS_POLICY_NET_EXCESS_V1
label_column = net_excess_return_bps
label_status = MATURED
label_transform = TRAIN_MEDIAN_MAD_AFFINE_V1
objective = huber
huber_alpha = 0.90
early_stopping_metric = l1
primary_metric = mean_daily_net_excess_return_bps
tie_break = family_id_seed_ascending
resource_max_rss_bytes = 8589934592
```

request 必须精确绑定：P0-C bundle/manifest、Program/binding/package/manifest、style profile、feature schema、shadow/cost/split policy、Qlib/H5/suspend roots及 cutoff、repository root/commit、两个 family、三个 seed、exact P0-D reference bundle 和 exact P0-E diagnostic reference bundle。

request 还必须记录 `model_information_cutoff_trade_date`、`latest_training_decision_trade_date`、`latest_training_label_observation_trade_date` 和 `experiment_lineage=(P0-D,P0-E,P0-F)`。`latest_training_label_observation_trade_date` 必须从MATURED rows的 `label_information_end` 最大值读取；`model_information_cutoff_trade_date` 取全部训练feature、HMM和label可见信息截止日的最大值，不能只使用decision date。当前exact P0-C身份下，decision截止为 `2026-02-02`、MATURED label information截止为 `2026-03-10`。P0-D/P0-E已经使用同一P0-C开发样本，因此P0-F CPCV只能支持“进入Stage B继续验证”，不能宣称新的独立泛化证据。

### 5.2 Continuous label transform

每条 CPCV path 只从 train rows 计算：

```text
location_bps = median(train net_excess_return_bps)
scale_bps = median(abs(train net_excess_return_bps - location_bps))
standardized_target = (net_excess_return_bps - location_bps) / scale_bps
predicted_policy_net_excess_return_bps = predicted_standardized * scale_bps + location_bps
```

不执行 clipping 或 winsorization，避免再次压平大盈利幅度；Huber loss 固定承担异常值稳健性。validation 必须复用该 path 的 train location/scale，禁止用 validation 重新拟合。非有限收益、空 train/validation、`scale_bps <= 0`、非有限预测或逆变换失败全部 fail closed。

final refit 只使用 exact P0-C 中全部 `MATURED` rows 重新拟合 full-train location/scale；context-only、censored、自然 forward 和历史回放 rows 不得进入。

### 5.3 Model families

固定 family 与 P0-D 一致，唯一变化是训练目标：

| family | features | fixed parameters |
|---|---|---|
| `FAMILY_POLICY_UTILITY_CORE` | P0-D CORE，不含 HMM | leaves 15；lr 0.03；min leaf 80；feature/bagging 0.8；L1 0.1；L2 1.0；threads 4；max rounds 600；early stop 60 |
| `FAMILY_POLICY_UTILITY_CORE_HMM` | P0-D CORE + HMM | 同上 |

seed 固定为 `20260813, 20260817, 20260823`。family/seed/path 共 `2 × 3 × 28 = 168` 个 trial-path。不得根据首轮结果增加 family、seed、rank guard、label transform 或目标函数。

### 5.4 Deterministic rank contract

每个 decision date 必须保留 exact Selection Top20，一行不少、一行不多。排序键固定为：

```text
predicted_policy_net_excess_return_bps DESC
selection_effective_rank ASC
instrument ASC
```

产生 `entry_priority_rank=1..20`。Top5 shadow portfolio 的 entry 按 utility rank，退出继续使用 `selection_exit_rank=selection_effective_rank`。utility prediction 只改变 challenger entry priority，不改变候选召回和任何 policy transition。

### 5.5 Candidate and portfolio metrics

候选级必须报告：MAE、RMSE、按日 Spearman、Top5-vs-rest realized utility spread、预测分布和 Selection rank bucket attribution。它们用于判断模型学到了什么，不选择 winner。

winner 只按 validation shared-policy `mean_daily_net_excess_return_bps` 选择，并报告：

- mean daily net excess/net return；
- maximum drawdown；
- mean turnover；
- completed episode hit rate、mean/median episode return；
- coverage、cash days、entry/exit count；
- Selection/P0-D/P0-E/HMM/random/Candidate20 对照；
- 8-block score、168 trial-path、70-partition PBO。

### 5.6 Advancement contract

Stage A 训练前固定以下条件，必须全部满足才允许 Stage B：

1. 28-path candidate minus exact P0-D mean primary metric `> 0 bps`。
2. candidate 对 exact P0-D path win rate `> 0.50`，tie 不计 win。
3. candidate 对 Selection Top5 mean primary metric `> 0 bps`。
4. 28-path paired mean `maximum_drawdown_difference >= 0`，即回撤不比 P0-D 更深。
5. 28-path paired mean `mean_turnover_fraction_difference <= 0`，即换手不比 P0-D 更高。
6. 28 个 path 身份完整唯一；任何 path 不可计算均为 `NEGATIVE_STOP_INCOMPLETE_CPCV`，不能缩减路径冒充通过。

PBO、candidate MAE/Spearman 和已消费回放不是 advancement gate。若任一条件失败，实验状态为 `NEGATIVE_STOP_NOT_ADVANCED`：保留真实 bundle/receipt，停止运行时、descriptor 和历史回放开发，不追加调参。

### 5.7 Conditional runtime contract

Stage B 新角色固定为：

```text
model_role = policy_utility_ranker_with_meta_label_confidence
```

新 bundle 自包含 `utility_model.txt` 和与 exact P0-D winner 字节/hash一致的 `confidence_model.txt`。entry priority 只按 utility model；既有 `take_probability/skip_probability/advisory_model_confidence` 继续由 exact P0-D binary model 产生，禁止把 regression score sigmoid/CDF 后伪装成概率。

bundle 必须分别保存 `utility_feature_schema.json` 和 exact P0-D `confidence_feature_schema.json`。共享 feature builder 生成完整P0-D v1 superset后，两个model各自按自己的trained feature names、categorical vocabulary和缺失指示构建矩阵；禁止假设utility winner必含HMM，也禁止以utility CORE schema裁剪confidence model输入。Stage B还必须自包含并核验P0-D fresh HMM continuation assets，因为confidence model的exact winner是CORE_HMM。

候选输出保留旧字段，并新增：

```text
predicted_policy_net_excess_return_bps
entry_priority_score_kind = PREDICTED_POLICY_NET_EXCESS_RETURN_BPS
```

utility role 中 `advisory_model_score` 与通用 rank-score 语义一致，等于 `predicted_policy_net_excess_return_bps`；`take_probability` 仍是独立字段且只来自 exact P0-D binary model。排序权威字段是 `entry_priority_rank`，`entry_priority_score_kind` 防止消费者混淆分值单位。旧 `quality_reranker`、`meta_label_take_skip_confidence` descriptor/bundle/API bytes 不变。

历史回放的 `advisory_p0d_historical_forward_replay_v1` bytes和loader保持不变；utility role使用显式 `advisory_historical_forward_replay_v2` union，新增 `model_role`、`predicted_policy_net_excess_return_bps` 和 `entry_priority_score_kind`，同时保留由P0-D产生的三项概率/置信度字段。v2必须证明exact Top20、rank 1..20、score-kind/value一致和概率和为1；不得把v2写成旧v1后丢失utility语义。

### 5.8 Evidence classification

- 当前已消费24决策日窗口固定为 `HISTORICAL_REPLAY`，永不恢复为 OOT。
- 新历史 OOT 必须在读取该窗口 labels/returns 前持久化 window identity 和 `UNCONSUMED_FOR_MODEL_SELECTION`；一旦用于选择即永久 consumed。
- 任何历史 replay/OOT 的首个 `decision_as_of_trade_date` 必须严格晚于 `model_information_cutoff_trade_date`；训练label所需的最后退出/价格观测也必须不晚于该cutoff。
- 自然 forward observation/outcome 不回填；只有目标日权威 open、20日最大 maturity 和同一 shadow policy 能形成独立 future OOS。
- Stage A advancement 通过后才可运行历史 replay；历史 replay 无论结果如何都不能改变 Stage A winner。

## 6. Leakage and PIT boundary / 未来数据与身份边界

- 特征 cutoff 固定为 `decision_as_of_trade_date`；target 日及其后价格、MFE/MAE、exit reason、episode path 只可用于 label/evaluation。
- CPCV purge/embargo 继续覆盖最长20交易日 policy window；train/validation label span 不交叉。
- label transform location/scale、categorical vocabulary、early stopping 和 final rounds 均不得读取 validation 之外的未来块或历史 replay。
- fresh HMM 只用每个 decision date 当时可见数据；无 HMM 的日期按既有 typed unavailable/missing-indicator 合同处理，不静默回填未来状态。
- request 绑定 clean tracked source commit；root/commit/hash、bundle/reference、path roster 或 artifact identity 不符均 fail closed。
- 训练只读 QE/Qlib/H5/PKL artifacts；Stage B 正式推理才读取数据库 decision-cutoff 数据。本任务不新增训练数据库路径。
- future-poison测试必须在training cutoff之后添加极端行情/标签，并证明request、transform、model inputs和所有较早validation prediction不变；cutoff相等或倒序直接拒绝。

## 7. API / UI / DB / Runtime impact

| area | Stage A | Stage B（仅 advancement PASS） |
|---|---|---|
| API | none | 现有 candidate payload 向后兼容新增 utility 字段 |
| UI | none | 不要求 UI 变更；旧概率展示保持原语义 |
| DB/DDL/DML | none | none |
| backend source | 新离线模块/CLI | resolver/inference/replay 支持新显式 role |
| backend restart | none | 仅未来源码合入后的运行时启用另行由用户执行 |
| production descriptor | untouched | 默认仍 untouched；任何旋转需单独授权 |
| dependency | none | none |

## 8. Failure semantics / 失败语义

| reason code | condition |
|---|---|
| `ADVISORY_POLICY_UTILITY_REQUEST_INVALID` | request/hash/family/seed/objective 非冻结值 |
| `ADVISORY_POLICY_UTILITY_SOURCE_INVALID` | P0-C/reference/feature/policy identity 不一致 |
| `ADVISORY_POLICY_UTILITY_PATH_NOT_COMPUTABLE` | path 无完整 train/validation mature rows |
| `ADVISORY_POLICY_UTILITY_TARGET_INVALID` | return/location/scale/transform 非有限或 scale<=0 |
| `ADVISORY_POLICY_UTILITY_PREDICTION_INVALID` | prediction shape/value/inverse transform/rank 不合法 |
| `ADVISORY_POLICY_UTILITY_REFERENCE_INVALID` | P0-D/P0-E path roster 缺失、重复或身份不匹配 |
| `ADVISORY_POLICY_UTILITY_BUNDLE_INVALID` | immutable bundle 文件、hash、schema 或自包含模型不一致 |
| `ADVISORY_POLICY_UTILITY_RUNTIME_ROLE_INVALID` | Stage B descriptor/model role 与 bundle 不匹配 |
| `ADVISORY_POLICY_UTILITY_NOT_ADVANCED` | Stage A advancement contract 未全部通过 |

失败返回结构化 context，但不保存密码、token、private key 或数据库响应正文。

## 9. Implementation Plan / 实施方案

1. 先实现 Stage A request、label transform 和 deterministic rank 的纯函数与直接测试。
2. 实现固定 Huber trial/final training，复用 feature builder 和 exact CPCV paths。
3. 复用 shared portfolio evaluator，补齐 P0-D/P0-E paired comparison、回撤/换手配对和 advancement receipt。
4. 发布自包含、不可变、repo-external offline bundle；exact retry 必须返回同一 bundle。
5. 使用 clean commit 在 WSL `rdagent-gpu` 完成真实 `168` trial-path，资源上限8GB。
6. 若 advancement 失败，记录负面结论并终止，不实现 Stage B。
7. 若 advancement 通过，再实现新 model role、双模型自包含 bundle、runtime formatter 和 replay union contract。
8. Stage B 只在隔离 model root 发布 descriptor；历史回放保持 `HISTORICAL_REPLAY`，自然 future OOS 独立积累。

## 10. Verification Plan / 验证方案

### 10.1 Contracts and transform

- 相同 functional request 得到相同 request id/hash；任一 identity/objective 变化得到新 identity。
- 手工矩阵证明 train median/MAD、inverse transform 和每日 deterministic rank 正确。
- validation 极端值不能改变 train transform；NaN/inf/zero MAD fail closed。
- label transform 不 clipping，正负大收益的顺序和可逆幅度保留。

### 10.2 Training and evaluation

- fixed `CORE/CORE_HMM × 3 seeds × 28 paths = 168`，无缺失/重复 trial-path。
- LightGBM 参数、objective、alpha、metric、rounds、seed 完全来自 frozen request。
- winner 由 shared-policy primary metric 决定，不由 regression loss、PBO 或历史回放决定。
- exact P0-D/P0-E path ids 一一配对；回撤、换手和 primary lift 方向断言正确。
- advancement 任一条件失败时 Stage B 调用明确拒绝。
- receipt显式披露P0-D/P0-E/P0-F顺序实验复用同一开发样本；CPCV不得标为independent OOS。

### 10.3 Runtime compatibility（条件）

- old P0-D descriptor/bundle bytes 和输出 contract 不变。
- utility role 同时校验两个模型、两份feature schema和P0-D HMM assets hash；utility 决定 rank，P0-D binary 决定概率。
- API 保留旧概率字段并新增 utility 字段；utility role 的 `advisory_model_score` 必须等于utility预测，且不得按 take probability 重排。
- historical replay v1 旧 artifact 可读取；新 role artifact 显式版本化且仍为 exact Top20。

### 10.4 Real experiment

- clean committed source、WSL `rdagent-gpu`、真实 P0-C labels/features、28 READY paths。
- peak RSS < 8GB；stage timing、row count、model/hash、winner/PBO/comparison/advancement receipt 完整。
- exact retry 不重训并返回同一 bundle。
- 只有 advancement PASS 才执行 isolated descriptor/replay；生产 descriptor readback 必须未变化。

## 11. Production Gates / 生产门禁

| gate | 本设计任务 | 后续实现默认 |
|---|---|---|
| source commit/push/PR/merge | 未授权，不执行 | 分别按用户授权 |
| DEV/production DDL/DML | `none` | `none` |
| backend/worker/scheduler restart | `none` | 用户持有，Stage A不需要 |
| dependency install | `none` | `none` |
| model training/artifact publish | 不执行 | 仅 Stage A 明确实验 root |
| production descriptor/binding activation | `out_of_scope` | 必须单独授权 |
| worktree/branch cleanup | 未授权 | 不执行 |

## 12. Rollout / rollback

### Rollout

1. 本 F2 设计和父蓝图先独立审核、validator 通过并合入。
2. 后续实现从最新主线创建新的 feature worktree，Stage A 代码/测试/真实训练作为一个封闭实验。
3. Stage A negative stop 是完整、可合入的真实实验结果，不触发 runtime、replay 或激活。
4. Stage A advancement PASS 才建立 Stage B revision；它仍只发布 isolated experimental descriptor。
5. 生产激活必须等待自然 future OOS 并由用户单独决定。

### Rollback

- 设计回滚只回退文档，不触碰 DB、runtime 或 artifact。
- Stage A bundle 内容寻址且 repo-external，不覆盖 P0-C/P0-D/P0-E。
- Stage B descriptor 使用 expected-current CAS 和不可变 snapshot；任何生产 rotation/rollback 均不在本任务授权内。
- 发现设计或实现语义错误时生成新 request/bundle，不修改既有 artifact。

## 13. Risks / 风险与处理

| risk | treatment |
|---|---|
| Huber 仍无法识别大盈利 | 不 clipping；报告 rank bucket、daily Spearman 和 top-vs-rest spread；失败即停止 |
| 连续收益尾部主导 | 固定 Huber alpha=0.90和train-only MAD，禁止结果后 winsorize 搜索 |
| regression loss改善但组合恶化 | winner/advancement只看shared-policy收益、回撤、换手 |
| 把回撤/换手变成人工审批 | 仅作为预注册研究 advancement contract，不改变生产审批或激活权 |
| utility score伪装概率 | `advisory_model_score`/utility字段使用bps分值；exact P0-D binary model只生成独立概率字段 |
| 双模型 bundle产生隐式依赖 | Stage B bundle自包含两个模型并校验reference bytes/hash |
| 已消费窗口继续调参 | Stage A通过前不回放；任何回放固定为HISTORICAL_REPLAY且不改winner |
| 多轮P0-D/E/F复用同一CPCV夸大泛化 | request/receipt记录experiment lineage；只把CPCV用于Stage B advancement，自然future OOS才是独立证据 |
| replay日期早于训练信息cutoff | 固定model information cutoff并要求replay首日严格晚于cutoff，增加future-poison测试 |
| 负面模型仍过度工程化 | Stage A negative stop明确终止Stage B |
| 新 role破坏旧消费者 | 新显式model_role和union contract；旧role bytes/行为回归测试 |
| 代码成功冒充模型成功 | design/source/training/advancement/replay/activation分别报告 |

## 14. Design Acceptance Index

| ID | requirement |
|---|---|
| F-801 | 失败归因直接支持从 binary weighting 转向连续 policy utility，而非开放调参 |
| F-802 | request精确冻结P0-C/P0-D/P0-E、objective、family、seed、path和source identity |
| F-803 | median/MAD transform仅用path train、可逆、不clipping、非法输入fail closed |
| F-804 | 固定Huber CORE/CORE_HMM×3 seeds×28 paths，无结果后搜索 |
| F-805 | utility只改变exact Selection Top20 entry priority，selection exit和shadow policy不变 |
| F-806 | candidate diagnostics与shared-policy winner选择分离 |
| F-807 | exact P0-D/P0-E paired comparison和PBO完整，advancement contract预注册 |
| F-808 | advancement失败完整终止Stage B，不以运行时工程冒充模型进展 |
| F-809 | Stage B概率来自exact P0-D binary；双模型/双schema/HMM资产自包含，utility score不伪装概率，旧role兼容 |
| F-810 | 历史回放只在advancement通过后运行且证据分类不冒充future OOS |
| F-811 | immutable self-contained bundle、clean commit、WSL、8GB和exact retry合同完整 |
| F-812 | 零DDL/DML、零默认生产激活；重启、descriptor和清理保持独立授权 |
| F-813 | 定向测试、F2 validator、scope/diff和三轮设计审核定义完整 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-801 | 本设计 §§1,3 | artifact: `docs/architecture/advisory_p0d_short_rebound_meta_label_f2_design_20260824_return_aware_challenger.md` | design_ready | none |
| F-802 | `policy_utility_contracts.py` | `backend/tests/advisory_model_first/test_policy_utility_contracts.py` | design_ready | none |
| F-803 | `policy_utility_training.py` | `backend/tests/advisory_model_first/test_policy_utility_training.py` | design_ready | none |
| F-804 | request/training/pipeline | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-805 | rank formatter + shared evaluator | `backend/tests/advisory_model_first/test_policy_utility_training.py`; `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py` | design_ready | none |
| F-806 | pipeline candidate/portfolio metrics | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-807 | paired comparison + PBO | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py`; `backend/tests/advisory_model_first/test_policy_pbo.py` | design_ready | none |
| F-808 | advancement receipt and stage guard | `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | design_ready | none |
| F-809 | conditional bundle/resolver/inference | `backend/tests/advisory_model_first/test_policy_utility_runtime_inference.py`; `backend/tests/advisory_model_first/test_dynamic_model_binding.py` | design_ready | none |
| F-810 | conditional historical replay union | `backend/tests/advisory_model_first/test_historical_forward_replay.py` | design_ready | none |
| F-811 | WSL CLI and bundle | `backend/tests/advisory_model_first/test_policy_utility_bundle.py`; artifact: future isolated `resource_report.json` | design_ready | none |
| F-812 | boundary assertions | `backend/tests/advisory_model_first/test_meta_label_boundaries.py`; `backend/tests/advisory_model_first/test_policy_utility_runtime_inference.py` | design_ready | none |
| F-813 | complete design diff | validation-receipt: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0f_policy_utility_ranker_f2_design_20260824.md --tier F2` | design_ready | none |

矩阵中的 `design_ready` 仅表示设计条款、目标实现位置和验证 oracle 已冻结；它不表示代码、模型或运行时已经实现。

## 16. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：Stage A 使用真实 P0-C labels/features 和完整28 paths；负面停止是预先定义的完整实验结果，不用fixture或缩减路径冒充成功。
2. **禁止静默错误**：身份、path、transform、prediction、reference、bundle和role不一致全部typed fail closed；不以fallback Selection排序伪装utility成功。
3. **禁止改变业务逻辑**：候选召回、Selection exit、policy、成本和生产baseline不变；唯一实验变量是连续utility entry priority。
4. **禁止私增门禁审批**：advancement是训练前冻结的研究阶段合同，不是生产审批；生产激活仍只有用户决策且没有新增角色门禁。

## 17. Review record / 审核记录

- Round 1（完整性/泄漏）：发现初稿只写泛化cutoff，未精确绑定P0-C `label_information_end`，且未披露P0-D/E/F连续复用同一开发样本。已补充decision/label/model information三种cutoff、experiment lineage、replay首日严格晚于cutoff和future-poison测试；复审通过。
- Round 2（业务/实验偏差）：发现初稿令utility role的`advisory_model_score`继续表示take probability，会破坏通用rank-score语义；同时单一feature schema无法同时覆盖可能为CORE的utility winner和固定CORE_HMM confidence model。已修正为utility bps score，补充双模型/双schema/P0-D HMM资产自包含，并明确candidate loss、shared-policy winner、PBO和advancement分离；复审通过。
- Round 2B（父蓝图/兼容）：发现父蓝图F-151仍把已完成P0-D descriptor写成待PR，且新role replay版本不明确。已用PR #3726及运行时事实纠正蓝图，冻结旧replay v1不变和utility replay v2 union；复审通过。
- Round 3（合规/生产边界）：逐项复核DESIGN-COMPLIANCE-001，确认Stage A negative stop是完整实验、全部错误fail closed、唯一业务变量不漂移、无新增生产审批；F2 validator PASS、quality guardrail 0 finding、`git diff --check`通过。

任一后续范围变化必须先更新F-801..F-813及父蓝图，再从受影响轮次重新审核；本记录不替代未来实现和真实模型结果审核。

## 18. Completion definition / 设计完成定义

- 本设计与父蓝图对P0-F目标、阶段、指标、停止条件、证据分类和授权边界一致。
- F-801..F-813 每项均具有精确计划实现位置和直接验证 oracle，无未批准 gap。
- F2 validator、`git diff --check` 和三轮人工设计审核通过。
- 不存在把回归分数称为概率、把候选loss称为组合成功、把历史 replay 称为future OOS或把Stage A负面称为partial的语义。
- 设计内容满足后续独立实现PR的输入要求；本任务仍不声明源码、模型、运行时或生产完成。
