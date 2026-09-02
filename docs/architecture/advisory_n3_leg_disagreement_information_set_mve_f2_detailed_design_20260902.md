# Advisory N3 腿间共识/分歧信息集扩展 MVE F2 详细设计

> 版本：v1.2
> 日期：2026-09-02
> Feature tier：F2
> 状态：IMPLEMENTED_LOCAL_VERIFIED_FORMAL_PENDING
> objective contract：`ALPHA_RANKING`
> study type：`LEARNABILITY_AUDIT`
> decision use：`NAVIGATION_ONLY`

## 1. Background / 背景与事实入口

1. N3 首批 QE 上游 Alpha MVE 正式 bundle `09137f0c...`为 `24/24/24/0`。六个低父包相关、family-wise RankIC 下界为正的导航信号随后进入独立父包增量 overlay。
2. 父包增量 overlay 正式 request `advn3ovlreq_152dc894211c967347155ceb`、bundle `fdca2130...`已在 clean `origin/main@7bbd56c6...`完成；结果为 `24/24/24/0`。bundle inspect 与 exact retry 均通过，registry duplicate no-op、route exact no-op。
3. overlay 失败不是干预不足：五个信号覆盖 386/386 日，regime 信号覆盖 158/386 日；但 24 个 trial 的 family-wise Top5 成本后 lift 下界均不大于 0。继续调相同信号权重、符号、loss 或窗口违反已冻结路线。
4. route 已唯一指向 `N3_ALPHA_INFORMATION_SET_EXPANSION_MVE`。本任务只检验新的腿间共识/分歧信息，不并行启动分钟数据、N2-B 独立包、Entry、Exit 或新模型族。
5. N2-A 正式三腿 panel 在相同开发窗口提供 `LSTM_ONLY`、`FUNDGROWTH_ONLY`、`IC_WEIGHTED_PARENT`逐股分数。只读可行性审计确认 1,710,301 行、386 日、4,503 股票三列均 100% finite；LSTM/FUND 日内 rank 相关仅约 0.186，分歧非退化。
6. 只读可行性审计确认N2-A `IC_WEIGHTED_PARENT`与N2-B `CURRENT_IC_PARENT`在全部1,710,301个同键行上score、outcome和known状态精确一致，最大绝对差为0。正式MVE不新增N2-B依赖，而是把N2-A的1,709,387个known行与父overlay bundle中的current-parent known panel做exact parity硬门禁；914个unknown行及4,055个known但nonfinite行保留在逐日候选池和OOF评分中，只从训练标签与对应日的不可评价指标中排除。

## 2. Scope / 目标与成功边界

交付一个固定信息集、固定模型族、严格 cross-fit 的 learnability MVE，回答唯一问题：

> 在当前父包分数之外，显式的腿间一致、矛盾和非线性交互是否能在相同 PIT/H20/成本政策下产生可学习的增量 Top5 经济价值？

交付范围：

1. 冻结 N2-A、N1 CPCV 和父 overlay 三组来源身份；`prepare`不得训练或读取结果统计。
2. 构造一个线性腿分数 comparator 和一个共识/分歧 expanded 模型；两者使用完全相同的 Ridge、预处理、fold 和标签。
3. 在 N1 原 8 block、28 READY path、20 日 embargo 上为全部1,710,301个source row生成每行恰好7个OOF prediction的平均值；只有1,705,332个finite-evaluable row可进入各fold训练。
4. 报告相对当前父包及线性 comparator 的 paired RankIC、Top5 成本后 lift、干预支持、换手、MDE 和 family-wise 区间。
5. 发布 immutable bundle、append-only registry record、单页 route 和 exact retry no-op。

本 MVE 的成功只允许产生一个 `NAVIGATION_ONLY` candidate 和后续 confirmation 设计入口；不得生成可部署模型或激活业务。

## 3. Non-goals / 非目标与禁止项

- 不搜索 Ridge alpha、solver、fold、seed、阈值、特征子集、标签、权重、方向或窗口。
- 不回选父 overlay 的 24 个 trial，不改写其 `selected=0`。
- 不读取 sealed holdout，不把本开发窗口称为 future OOS、confirmation 或 activation evidence。
- 不访问 DB、网络、Tushare、Qlib daily/minute；不读取分钟 Bin。
- 不写因子库、StrategyPackage、Selection、Advisory runtime、descriptor、资金权重、Paper/QMT 或订单。
- 不 final-refit，不保存 estimator/joblib，不接 API/UI，不启动、停止或重启任何进程。
- 不把两个模型的多个 fold 计为独立发现；也不得把 comparator 隐藏为“非 trial”。

## 4. Architecture / 架构与数据流

```text
N3 parent overlay selected=0 receipt
                    |
N2-A full_universe_signal_outcomes.parquet
  three T-visible leg scores + frozen H20 outcome
                    |
N1 n1_label_interval_cpcv.json (8 blocks / 28 paths / 20d embargo)
                    |
same-date canonical ranks + fixed interaction features
                    |
2 frozen Ridge trials, train-fold preprocessing only
                    |
7-path mean OOF scores per row
                    |
paired daily RankIC / Top5 net lift / support / churn / MDE
                    |
immutable bundle -> registry -> route
```

正式 request 只能从 clean `HEAD == origin/main`生成。运行时只消费 request 绑定的本地文件，不重新查询外部来源。

## 5. Contracts / 输入与身份合同

### 5.1 必需 bundle

1. N3 parent overlay bundle `fdca2130...`：必须 inspect valid、`24/24/24/0`、`next_task=N3_ALPHA_INFORMATION_SET_EXPANSION_MVE`、`NAVIGATION_ONLY`、sealed/deployable/runtime/business-write 全 false；消费`request.json`、`overlay_receipt.json`、`daily_metrics.parquet`、`registry_record.json`和`manifest.json`，并通过该request直接绑定父QE bundle的`score_panel.parquet`作为current-parent逐股parity源。overlay输出只保存`parent_rank`而不复制原始score/outcome，不得错误地把它当训练标签源。
2. N2-A bundle `6784df1a...`：必须通过原 bundle manifest/readback；experiment 为 `ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT`，只消费：
   - `full_universe_signal_outcomes.parquet`
   - `request.json`
   - `audit_receipt.json`
   - `registry_record.json`
   - `manifest.json`
3. N1 bundle `74827d03...`：必须 inspect valid，只消费 `n1_label_interval_cpcv.json`、`learnability_daily.parquet`、`request.json`、`manifest.json`和 learnability receipt 的 split identity；`learnability_daily`仅提供已冻结的T日regime映射，不消费其模型lift作为本MVE标签或特征。

### 5.2 主键、窗口与标签

- 唯一键：`decision_as_of_trade_date + instrument`。
- 信号窗口：`2024-07-04..2026-02-02`，386 个决策日。
- 股票代码必须 canonical uppercase；重复键、日期漂移或额外 arm 列直接失败。
- 特征源列：`score__LSTM_ONLY`、`score__FUNDGROWTH_ONLY`、`score__IC_WEIGHTED_PARENT`，必须全 finite。
- 标签：N2-A 已冻结的 `economic_net_excess_bps`。1,709,387个`outcome_known=true`行中，1,705,332个finite label进入训练与逐日RankIC；4,055个known但nonfinite的正常缺失和914个unknown行仍保留在逐日排序、Top5候选与OOF输出中。两类缺失都不填零、不在排序前删除股票、不删除整个日期、不阻断实验；若任一Top5成员标签不可用，该模型当日Top5指标为typed unavailable，而不是用少于5只股票的均值冒充Top5。
- 目标 H20、买卖成本、capacity haircut、benchmark、PIT universe、baseline/shadow policy identity完全继承 N2-A/N1，不重新定义。
- N2-A的known子集必须与父overlay绑定的current-parent panel在1,709,387个键上exact parity，NaN位置也必须相同；覆盖或逐值差异均为source identity失败。只有1,705,332个finite-evaluable行进入cross-fit训练；全部1,710,301行进入validation scoring并各自获得7个OOF prediction，4,055个nonfinite-known和914个unknown均不得填零或混入训练。

### 5.3 组合数据身份

request 同时记录 N2-A dataset identity、N1 split policy hash、父 overlay dataset/policy identity和全部 evidence refs。新 `dataset_identity`是这些冻结来源身份的 canonical hash；`policy_identity`必须与父 overlay 一致。任一文件 hash、size、row count、bundle id 或关联身份变化均 fail closed。父request遗留的registry/route路径必须在prepare时规范化到当前宿主OS，禁止Windows进程把`/mnt/f/...`误当成本地相对路径，也禁止WSL进程直接消费盘符路径。

## 6. 决策时钟与特征合同

所有特征只使用 T 日当时已经存在的三腿 score。同日 rank 仅在该日 canonical panel 成员内计算，`rank(method="average", pct=True, ascending=True)`；不得跨日归一化。

### 6.1 Comparator 特征

`LEG_LINEAR_COMPARATOR_V1`固定 3 项：

1. `parent_rank_pct`
2. `lstm_rank_pct`
3. `fund_rank_pct`

### 6.2 Expanded 特征

`LEG_DISAGREEMENT_EXPANDED_V1`包含 comparator 3 项及以下 5 项：

1. `leg_rank_signed_gap = lstm_rank_pct - fund_rank_pct`
2. `leg_rank_abs_gap = abs(leg_rank_signed_gap)`
3. `leg_rank_consensus_min = min(lstm_rank_pct, fund_rank_pct)`
4. `leg_rank_consensus_product = lstm_rank_pct * fund_rank_pct`
5. `parent_rank_x_agreement = parent_rank_pct * (1 - leg_rank_abs_gap)`

以上 roster、名称、顺序和公式进入 schema hash。不存在可选特征或 silent fallback。特征 builder 的 future/label poison 测试必须证明修改 outcome、exit、T+1 及以后字段不会改变任何 feature value/hash。

## 7. 固定模型与 cross-fitting

### 7.1 两个诚实计数的 model trial

| trial | 作用 | 特征 | 可被选择 |
|---|---|---|---|
| `N3_LEG_LINEAR_COMPARATOR_V1` | 判断简单重估三腿是否已解释增量 | §6.1 | 否，仅 comparator |
| `N3_LEG_DISAGREEMENT_EXPANDED_V1` | 检验非线性共识/矛盾信息 | §6.2 | 是，最多一次 |

`planned/generated/evaluated=2/2/2`。两个模型、所有 28 path 均使用：

- estimator：`sklearn.linear_model.Ridge`
- alpha：`100.0`
- solver：`lsqr`
- fit_intercept：`true`
- numeric preprocessing：train-fold `StandardScaler`
- label：原始 `economic_net_excess_bps`，不按结果 winsor、重采样或改方向
- random model seed：不适用；bootstrap seed `20260902`

### 7.2 CPCV 纪律

- 精确复用 N1 的 8 block、28 READY path、20 日 embargo；不得重算或换 split。
- 每个 path 的 train/validation date 必须不相交，且 source date 必须在 N1 block map 中。
- scaler 只 fit train；Ridge 只 fit train known outcomes。
- 每个source row必须成为validation恰好7次；实现按row累加prediction sum/count，禁止materialize约12M行path-level副本。标签可用性只约束train mask，不得改变validation候选集合。
- comparator 与 expanded 使用同一 train/validation row identity；任一 multiplicity、非有限预测、行序或 key 漂移直接失败。

## 8. 评价、支持度与单次选择

### 8.1 每日配对指标

对 current parent、linear comparator、expanded 三个 score，每日计算：

- Spearman RankIC；
- Top5 `economic_net_excess_bps`均值及显式`top5_evaluable`；任一Top5成员标签不可用时整项指标为typed unavailable，禁止skipna形成部分持仓均值；
- Top5 instrument set、与父包/linear 的 replacement count；
- 相邻决策日 Top5 churn；
- coverage 和 finite fraction。

expanded 同时报告相对 parent 与 linear 的 RankIC delta、Top5 lift。推断使用 20 日 moving-block bootstrap、2000 repetitions、seed `20260902`。model trial诚实计数仍为2；四个必过主比较（两种指标×两个baseline）另设`familywise_hypothesis_count=4`，统一使用 Bonferroni `alpha=0.05/4`。DSR按2个模型trial诊断，不替代配对经济门槛。

父基线daily parity对RankIC与Top5 churn逐日精确比较；Top5经济值仅在当前父Top5五个标签全部可评价时逐值精确比较。只读预检发现`2025-04-14`和`2025-10-23`两个父Top5含正常nonfinite label：新合同必须保留这两日并标记`parent_top5_evaluable=false`/value NaN，不得复用旧产物skipna所得的4只股票均值。paired inference只消费四项增量指标均finite的日期，并在receipt同时报告总决策日和可评价日。

### 8.2 干预支持

expanded Top5 与 parent Top5 不同才算 intervention。预注册最低值：

- evaluable days `>=382`
- intervention days `>=60`
- intervention fraction `>=0.25`
- N1 regime map中每个实际出现的 regime intervention days `>=20`

缺少 regime 的日期保留在总体评价并单独计数，不得填充或删掉整个日期。regime映射只取N1 `learnability_daily.parquet`中的`decision_as_of_trade_date, regime`两列；任何其它learnability预测、lift或未来字段不得进入本MVE。

### 8.3 Candidate eligibility

expanded 只有同时满足下列条件才可一次选择：

1. source、feature、CPCV、OOF、coverage 和 parent Top5 parity 全通过；
2. 干预支持满足 §8.2；
3. 相对 parent 的 family-wise RankIC delta 下界 `>0`；
4. 相对 parent 的 family-wise Top5 net lift 下界 `>5 bps`；
5. 相对 linear comparator 的 family-wise RankIC delta 下界 `>0`；
6. 相对 linear comparator 的 family-wise Top5 net lift 下界 `>0`；
7. 无非有限、退化、identity、PIT 或资源错误。

若 selected=1，next task=`N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN`；若 selected=0，next task=`N3_MINUTE_INFORMATION_SET_MVE`。不得在同一 frontier 重新选特征、alpha或阈值。

## 9. Artifact、registry 与 route

bundle 固定成员：

- `request.json`
- `feature_schema.json`
- `feature_panel.parquet`
- `oof_score_panel.parquet`
- `fold_diagnostics.parquet`
- `daily_metrics.parquet`
- `model_summary.json`
- `frontier_receipt.json`
- `source_identity_receipt.json`
- `resource_report.json`
- `learnability_receipt.json`
- `registry_record.json`
- `manifest.json`

manifest 绑定成员 SHA256、size 与 parquet row count。发布后完整 readback 才允许 registry append。exact retry 必须复用同一 bundle，registry duplicate no-op，route exact no-op。

registry：

- experiment：`ADVISORY-N3-LEG-DISAGREEMENT-LEARNABILITY-V1`
- stage：`N3_ALPHA_INFORMATION_SET_EXPANSION_LEG_DISAGREEMENT`
- study：`LEARNABILITY_AUDIT`
- objective：`ALPHA_RANKING`
- decision use：`NAVIGATION_ONLY`
- unique variable：`FIXED_LINEAR_LEGS_VS_FIXED_NONLINEAR_LEG_CONSENSUS_DISAGREEMENT`
- trial count：`2/2/2/0|1`
- consumed window：`P0C_DEVELOPMENT_CONSUMED_20240704_20260202`

route 只记录研究导航，不构成模型激活或交易输入。

## 10. 资源、错误与安全边界

- concurrency=1；RSS上限16 GiB；临时输出上限16 GiB；wall time仅遥测，`null`不自动停止。
- 输入 panel 只读取一次；feature/OFF数组优先 float32/float64必要精度，禁止保存28份预测副本。
- typed reason code：
  - `ADVISORY_N3_LEG_MVE_REQUEST_INVALID`
  - `ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH`
  - `ADVISORY_N3_LEG_MVE_PIT_LEAKAGE`
  - `ADVISORY_N3_LEG_MVE_CPCV_INVALID`
  - `ADVISORY_N3_LEG_MVE_OOF_INVALID`
  - `ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED`
  - `ADVISORY_N3_LEG_MVE_BUNDLE_INVALID`
  - `ADVISORY_N3_LEG_MVE_RESOURCE_LIMIT_EXCEEDED`
- 正常 unknown outcome 以 typed coverage保留；不得 broad exception 后继续成功。

## 11. Implementation plan / 实现文件与顺序

精确源码范围：

- `backend/services/advisory_model_first/leg_disagreement_contracts.py`
- `backend/services/advisory_model_first/leg_disagreement_pipeline.py`
- `scripts/advisory_leg_disagreement_mve_run.py`
- `backend/tests/advisory_model_first/test_leg_disagreement_contracts.py`
- `backend/tests/advisory_model_first/test_leg_disagreement_pipeline.py`
- `backend/tests/advisory_model_first/test_leg_disagreement_delivery.py`
- 本设计和主蓝图

实现顺序：合同与 source guard → feature builder/poison → CPCV OOF → paired evaluator → immutable delivery/CLI →真实只读预检 →重复审核。

## 12. Verification plan / 验证计划

1. 合同：两个固定 trial、schema hash、阈值、资源、安全 false gates和next-task关系不可override。
2. Source：N2-A/N1/parent bundle hash、关系、窗口、键、score coverage、outcome status与dirty source fail closed。
3. PIT：同日 rank只看同日成员；future/label poison不改变feature。
4. CPCV：28 READY、20日embargo identity、train/validation隔离、每row恰好7 OOF。
5. Model：相同 preprocessing/alpha/solver，expanded只多5项冻结feature；无final model。
6. Evaluation：parent parity、paired RankIC/Top5、family-wise interval、support/MDE/churn和0/1选择。
7. Delivery：manifest mutation、partial bundle、duplicate request、exact retry、registry/route no-op。
8. 全量：targeted tests、`advisory_modeling_backend`、Ruff/format、compile/mypy、L0、ownership、F2 validator、DESIGN-COMPLIANCE-001。

## 13. Risks and controls / 风险与控制

| 风险 | 控制 |
|---|---|
| 腿间分歧只是父包线性权重的重表达 | 固定 linear comparator；expanded 必须同时显著优于 parent 与 comparator |
| 同一开发窗口反复搜索 | 只允许预注册的两个 trial；结果仅 `NAVIGATION_ONLY`，不读 sealed holdout |
| 28 path 被误计成28次发现或产生巨量副本 | model trial固定为2；按row累加sum/count并要求恰好7 OOF |
| outcome或未来字段进入feature | exact roster、source projection及future/label poison不变性测试 |
| 高RankIC但Top5经济失败重演 | candidate强制Top5成本后lift下界，同时报告churn和支持度 |
| 实验失败后继续调alpha/特征 | frontier选择一次；selected=0固定转分钟信息集新lineage |
| 研究代码误接生产 | 无final model/API/runtime adapter；request/receipt/manifest均固定不可部署 |

## 14. Rollout, rollback and Production gates / 发布、回滚与生产门禁

Rollout仅是合入源码后从 clean main 生成一次冻结request并运行开发窗口审计。结果不接生产。

回滚只删除未发布的临时失败目录或回退源码PR；immutable bundle和append-only registry证据不改写。正式运行、backend restart、DDL/DML、descriptor/runtime activation均不是本任务步骤。

`production_ddl_gate=noop`，`backend_restart_gate=noop`，`runtime_activation_gate=noop`。

## 15. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-900 | 只从父overlay正式`selected=0`进入，禁止同族权重/loss继续搜索 |
| F-901 | N2-A/N1/parent三源身份、窗口、键、coverage和policy关系fail closed |
| F-902 | 3项comparator与5项expanded增量feature exact roster，T-visible且future/label poison不变 |
| F-903 | 两个model trial诚实计数；Ridge alpha/solver/preprocess固定，无搜索/重训激活 |
| F-904 | 精确复用28 CPCV path，每source row恰好7 OOF，只有finite-evaluable row进入训练，train-only preprocessing |
| F-905 | paired parent/linear评价、family-wise区间、MDE、干预日/比例/regime支持与单次选择 |
| F-906 | immutable bundle、manifest、registry、route、inspect和exact retry no-op |
| F-907 | sealed/DB/network/Qlib/minute/runtime/factor/package/position全部禁止，无restart/DDL |
| F-908 | selected=1只进confirmation设计；selected=0唯一转分钟信息集MVE |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-900 | `leg_disagreement_pipeline.prepare_leg_disagreement_request` | `backend/tests/advisory_model_first/test_leg_disagreement_delivery.py` parent selected-zero/next-task及跨OS registry/route规范化 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-901 | `leg_disagreement_pipeline._load_verified_sources` | `backend/tests/advisory_model_first/test_leg_disagreement_delivery.py` mutation/schema/key/window/coverage；真实source-only preflight `1,710,301/1,709,387/1,705,332` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-902 | `leg_disagreement_pipeline.build_leg_feature_panel` | `backend/tests/advisory_model_first/test_leg_disagreement_pipeline.py` exact formula and future/label poison cases | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-903 | `LegDisagreementModelSpecV1`; `run_leg_crossfit` | `backend/tests/advisory_model_first/test_leg_disagreement_contracts.py` fixed two-trial/alpha/solver cases | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-904 | `leg_disagreement_pipeline.run_leg_crossfit` | `backend/tests/advisory_model_first/test_leg_disagreement_pipeline.py` path isolation、全部source row七OOF、typed missing不入训练但保留评分 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-905 | `leg_disagreement_pipeline.evaluate_leg_models` | `backend/tests/advisory_model_first/test_leg_disagreement_pipeline.py` paired bootstrap/support/selection及Top5 typed-unavailable cases | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-906 | `prepare/run/inspect`; `_publish/_read/_deliver_bundle` | `backend/tests/advisory_model_first/test_leg_disagreement_delivery.py` manifest mutation and exact retry cases | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-907 | frozen request/receipt/manifest false gates | `backend/tests/advisory_model_first/test_leg_disagreement_contracts.py`；F2 validator；L0/ownership/static gates | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-908 | `LegDisagreementReceiptV1`; `_write_route_page` | `backend/tests/advisory_model_first/test_leg_disagreement_delivery.py` selected-zero/one route cases | IMPLEMENTED_LOCAL_VERIFIED | none |

## 17. DESIGN-COMPLIANCE-001

1. **设计目标逐项覆盖**：F-900至F-908均有实现目标、测试证据和状态，不以简化子集代替。
2. **代码逐项映射设计**：实现只允许使用§11文件范围；每项必须回填matrix中的精确symbol和test。
3. **测试逐项证明业务结果**：不仅验证schema，还必须证明PIT、7 OOF、parent parity、增量经济门槛、route与exact retry。
4. **差距显式保留**：任何未实现、未运行或仅探索性结论继续标记gap，不得报告complete、verified或可激活。
