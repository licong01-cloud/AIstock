# AIstock Advisory N3 QE 上游 Alpha MVE F2 详细设计 v1.2

> 日期：2026-09-02
> 状态：`FORMAL_COMPLETE_NO_STANDALONE_CANDIDATE`
> tier：F2
> objective contract：`ALPHA_RANKING`
> study type：`EXPLORATORY_SCREEN`
> production gates：restart/DDL/DML/runtime activation 均为 noop

## 1. Background / 当前事实与路线结论

1. N1 formal 在 386 个开发决策日中，全市场 Top5 赢家的 Top20/40/50 平均召回仅 `0.8808%/1.6062%/1.7617%`；Top50 平均每天只召回约 `0.088` 个全市场 Top5 赢家。Top20 内 clairvoyant Top5 上限很高，但固定 Ridge learnability 为欠功效探索结果，不能激活下游 ranker。
2. N2-A/N2-B 证明当前父包在现有包中最强，RankIC `0.12284`，Top5 H20 成本后净超额 `446.52 bps`；两个独立旧包均显著更弱。该结论说明不应替换成旧包，不改变候选流对全市场赢家的低绝对召回。
3. Entry formal 中只有动态 Q90 具备确认性干预支持，但其收益为显著负值；固定 3%/5% guard 的正点估计干预次数不足。perfect Entry 仍只是不具可学习性的 hindsight ceiling。
4. Exit oracle ceiling 为 `386.60 bps/episode`；固定 22 项 T-visible Ridge formal 的 point 为 `-56.81 bps/五槽entry-day`，95% CI `[-200.33,52.62]`、MDE `181.29`，support 充分但结论 `INCONCLUSIVE/NAVIGATION_ONLY`。
5. 因此按蓝图 N3 路由表，唯一主线冻结为 `N3_QE_UPSTREAM_ALPHA_MVE`。Entry/Exit 仅保留为以后扩信息集后的观察项；不并行训练第二条模型线。
6. 已合入的 preparation `advqeprep_7d28b455e667312d00cf54f9` 只冻结数据、grammar、24 proposal 预算和资源边界，本身不授权执行。当前用户已明确授权本长任务内的实验与模型训练；本设计把该授权落为一次、固定预算的 execution request，不扩展为自动演进平台。

## 2. 目标

交付一个开发窗口、单批次、24 proposal 的真实上游 Alpha MVE：

1. 生成不可变 N3 route receipt，绑定 N1、N2-A、N2-B、Entry、Exit、Exit learnability 和 QE preparation 的精确 hash/size，并证明只选择 `N3_QE_UPSTREAM_ALPHA_MVE`。
2. 冻结 6 个信号族、每族 4 个 proposal 的声明式 AST；编译器只解释 allowlist operator，不调用 `eval/exec/import/subprocess/network/DB`。
3. 使用冻结 Qlib daily、`static_factors.parquet` 和 N2-B 已消费开发窗口的 `CURRENT_IC_PARENT` 全截面 H20 成本后 outcome；禁止读取 sealed holdout。
4. 逐 proposal 输出完整日 RankIC、Top5 成本后净超额、相对父包 Top5 lift、coverage、churn、父包相关、95% block CI 和 24-trial family-wise 区间。
5. 按预注册的一次性 frontier 规则选择 0 或 1 个 exploratory candidate；结果只导航到后续 confirmation 设计，不形成模型、StrategyPackage、Selection 或运行时激活。
6. 发布 immutable bundle，append 一条 24-trial registry record，并把单页 route 更新为 N3 上游主线。

## 2.1 Formal result / 完成事实

- source PR：#4187；merge commit：`dc3ace36e3fbce80c7a0aa4438c111a3722ac2db`。
- request：`advqemvereq_28ac7e998080dd2258cf4c23`；request SHA256：`28ac7e998080dd2258cf4c23485efca2229b3797c37fe05174cb7911fef66da1`。
- immutable bundle：`09137f0c46c1fc3c40798e7ab63df0c0374cd11f65f75b7046cc9f939f099803`；inspect=`VALID`；exact retry复用同一bundle，registry duplicate-noop、route exact-noop。
- coverage：386个decision day、1,709,387条`CURRENT_IC_PARENT/outcome_known=true`；trial=`24/24/24/0`。
- resource：elapsed `91.205s`；peak RSS `7,422,263,296` bytes；temp `175,060,647` bytes；wall time仅遥测。
- result：24个proposal的family-wise Top5 lift下界均不大于0，因此按预注册frontier选择0个candidate；next task=`N3_ALPHA_INFORMATION_SET_REVIEW`。
- navigation signal：5个完整窗口proposal和1个下行regime proposal的family-wise RankIC下界为正且父包相关低于0.8。它们不能改判本轮selected=0，但为新lineage的父包小权重overlay提供导航依据；同窗overlay仍不得作为confirmation或activation evidence。

## 3. Non-goals

- 不实现 AlphaGen/AlphaAgent/LLM-MCTS 平台、通用 agent 编排、自动提示词自修改或无限循环。
- 不把 24 个 proposal 写入因子库、数据库或生产 StrategyPackage；不执行 DDL/DML。
- 不读取 Tushare、网络、数据库、sealed holdout 或未来 outcome 作为 factor 输入。
- 不训练 Top20 ranker、Entry/Exit 模型、组合权重、动态仓位或下单策略。
- 不把正 IC、单一 95% CI 或后验最好 proposal 当作 activation/confirmation evidence。
- 不处理历史证据归档、旧 root 或非阻碍性平台清理。

## 4. 架构与文件范围

```text
N1/N2 immutable receipts + registry + QE preparation
                         |
                  deterministic route gate
                         |
          frozen 24-proposal execution request
                         |
       Qlib daily + frozen static parquet + H20 outcomes
                         |
            allowlist AST vectorized evaluator
                         |
     24-wide score panel -> daily metrics/frontier
                         |
  immutable bundle + 1 registry row + N3 route page
```

新增范围：

- `backend/services/advisory_model_first/qe_alpha_mve_contracts.py`
- `backend/services/advisory_model_first/qe_alpha_mve_pipeline.py`
- `scripts/advisory_qe_alpha_mve_run.py`
- 三个对应 direct test 文件
- CI classifier/file ownership 精确 CLI 映射
- 本设计与主蓝图当前状态修订

不新建 scheduler、DB schema、API、UI、worker、cache service 或 ModelOps。

## 5. Contracts / N3 route gate

route request 必须绑定以下公开文件引用：

- N1 `oracle_receipt.json`、`learnability_receipt.json`、`quadrant_receipt.json`；
- N2-A `audit_receipt.json` 与 `arm_summary.json`；
- N2-B v2 `audit_receipt.json`、`arm_summary.json`、`pairwise_summary.json`、`arm_signal_outcomes.parquet`；
- N2 Entry/Exit `audit_receipt.json`、`entry_summary.json`、`entry_support.json`、`exit_summary.json`、`exit_support.json`；
- Exit learnability formal `learnability_receipt.json`；
- QE MVE `preparation.json`；
- trial registry 当前 hash、研究窗口/父包 policy identity、repository clean commit。

固定路由规则：

1. `minimum_global_top5_winner_recall=0.20`，经济含义为 Top50 候选平均每天至少包含 1 个全市场 Top5 winner；以 N1/N2-B Top50 recall 的 point 与上界校验。当前上界远低于 0.20，`candidate_recall_state=INSUFFICIENT`。
2. Ranker 只有 `direction_ready=true` 才可抢占主线；当前 N1 为 false。
3. Entry 只有非 oracle arm 同时 support sufficient 且 lift CI lower >0 才可抢占；当前无。
4. Exit 只有 fixed learnability `evidence_sufficient=true` 且 state=HIGH 才可抢占；当前 false。
5. recall insufficient 时无条件选择上游 Alpha MVE；不得因旧包较弱而选择旧包，也不得以 oracle ceiling 改走下游。

route receipt 为 0-trial 控制产物，不计入 DSR/PBO 的 24 个 model/signal trials。

## 6. Proposal 与表达式合同

固定信号族：

1. `PRICE_VOLUME_BEHAVIOR`
2. `MONEYFLOW_BEHAVIOR`
3. `FUNDAMENTAL_CHANGE`
4. `SECTOR_RELATIVE`
5. `CROWDING_DISPERSION`
6. `REGIME_CONDITIONED`

每族恰好 4 个 proposal，总计 24；每个 proposal 固定 `proposal_id/family/economic_hypothesis/expression AST/direction/source fields` 和 canonical hash。AST 限制：最多 64 nodes、深度 8、raw fields 8、lag/window 1..252。允许 operator 与 preparation 一致；本 MVE 首批只消费可完整实现的子集：`FIELD/CONST/ADD/SUBTRACT/MULTIPLY/SAFE_DIVIDE/ABS/SIGN/LOG1P_ABS/SQRT_ABS/CLIP/LAG/DELTA/TRAILING_SUM/TRAILING_MEAN/TRAILING_STD/TRAILING_MIN/TRAILING_MAX/SAME_DATE_RANK/SAME_DATE_ZSCORE`。

`TRAILING_CORR` 与 `INDUSTRY_DEMEAN` 虽在未来 grammar allowlist 中，但首批 proposal 不使用；调用未实现 operator 必须 typed fail，不得静默降级。proposal 间 AST hash 必须唯一。

## 7. 数据与 PIT 时钟

- signal dates：`2024-07-04..2026-02-02`，outcome cutoff：`2026-03-10`；只消费已登记 P0-C development window。
- lookback：从首个 signal date 向前至少 252 个 Qlib 交易日；所有 rolling/lag 以 instrument 分组且只读 `<=T`。
- daily source：冻结 Qlib snapshot；fields 为 open/high/low/close/volume/amount。
- static source：显式 `factor_root/static_factors.parquet`，request 绑定 file SHA256、size、schema hash 和所需字段；运行期间不访问 DB。
- target：N2-B `CURRENT_IC_PARENT`、`outcome_known=true` 的 `economic_net_excess_bps`；target 只在 factor score 完成后按 `(decision_date,instrument)` 一对一 merge。
- 每个signal date的`SAME_DATE_RANK/SAME_DATE_ZSCORE`只在N2-B outcome定义的当日canonical PIT成员键内计算；窗口内其他日期曾出现但当日不合格的股票即使有行情也必须mask，非成员极值毒化不得改变成员score。
- baseline：同一 outcome panel 的父包 `score`；Top5 baseline 和 proposal Top5 使用同一行、同一成本后 label。
- 停牌或正常缺失保留 NaN 与 coverage reason；不得删除整日、回填未来值或用 0 冒充。
- future poison：修改 T+1 及之后 daily/static，不得改变 T score/hash。

## 8. 固定 24 proposal roster

proposal 以代码中的声明式 AST 为权威，设计冻结下列经济主题，每项仅一个公式身份：

| family | proposals |
|---|---|
| PRICE_VOLUME_BEHAVIOR | 20日风险调整动量；5日反转×换手异常；20日量价趋势；区间压缩后的10日动量 |
| MONEYFLOW_BEHAVIOR | 主力5日相对20日加速；超大单5日相对20日加速；总净流入×换手；超大单占主力×筹码胜率 |
| FUNDAMENTAL_CHANGE | 营收/利润同比联合质量；毛利/净利联合质量；PB逆数×利润同比；流动资产占比×营收同比 |
| SECTOR_RELATIVE | 个股20日动量减行业20日收益；个股成交量趋势减行业成交量趋势；个股主力流入减行业净流入；个股PB逆数减行业PB逆数 |
| CROWDING_DISPERSION | 60日换手拥挤反向；20日波动之波动反向；成交量比率偏离反向；筹码胜率×小单强度 |
| REGIME_CONDITIONED | 上行市场20日动量；下行市场5日反转；下行市场主力流入；上行市场价值质量 |

方向在 AST 中显式体现，不允许结果后整体乘 -1；若方向错误，该 proposal 仍算一次已消费 trial。

## 9. 评价与一次性 candidate 规则

每个 proposal 必须报告：

- row coverage、finite fraction、decision day count；
- daily Spearman RankIC mean/median/std/positive fraction；
- proposal Top5 mean `economic_net_excess_bps`；
- 相对父包 Top5 的 paired daily lift；
- Top5 churn 与父包 same-date score Spearman；
- 20日 moving-block 95% CI；
- 24-trial Bonferroni family-wise block interval；
- daily lift Sharpe、skew、kurtosis 和 trial-count-aware DSR diagnostic。

一次性 frontier eligibility 全部满足才允许选 candidate：

1. evaluable days `>=382`；
2. finite row fraction `>=0.95`；
3. family-wise RankIC lower `>0`；
4. family-wise Top5 lift lower `>0`；
5. abs(parent same-date Spearman) `<=0.80`；
6. 无 PIT/duplicate/schema/degenerate failure。

若多个 eligible，只按 `familywise_top5_lift_lower DESC, familywise_rank_ic_lower DESC, proposal_id ASC` 选一次。其余结果和 frontier 一并冻结；confirmation 失败后不得回到本 frontier 重选。MVE 的 candidate 仍为 `NAVIGATION_ONLY`，不支持激活。

## 10. Bundle、registry 与 route

固定 bundle 成员：request、route receipt、source receipt、proposal roster、score panel、daily metrics、proposal summary、frontier receipt、resource report、registry record、manifest。

- manifest 绑定每个文件 SHA256/size/row_count、request/route/result identity；partial 或 mutation fail closed。
- registry 追加一条 `ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1` record，trial count=`24/24/24/0|1`，study=`EXPLORATORY_SCREEN`，decision use=`NAVIGATION_ONLY`。
- current route 更新为 active main line `N3_QE_UPSTREAM_ALPHA_MVE`；active auxiliary line=`NONE`；next task 取决于是否存在 candidate：有则 `N3_ALPHA_CANDIDATE_CONFIRMATION_DESIGN`，否则 `N3_ALPHA_INFORMATION_SET_REVIEW`。
- exact retry 必须复用同一 bundle、registry duplicate no-op、route hash 不变。

## 11. 资源与复杂度

- concurrency=1，RSS<=16GiB，temp<=32GiB，wall time 无停止门禁、仅遥测。
- source outcome 上界约171万行，lookback panel 约300万行；source join 必须 one-to-one/many-to-one 验证。
- AST 子表达式按 canonical hash 缓存，24 score 使用 float32 wide panel；禁止 materialize 24 份完整源表。
- cross-sectional 指标按日预分组；moving block 只在约386条 daily series 上执行。
- 每个阶段写进度与 RSS；失败不发布 manifest、不追加 registry。

## 12. Error contract

- `ADVISORY_N3_ROUTE_EVIDENCE_INVALID`
- `ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID`
- `ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH`
- `ADVISORY_QE_ALPHA_MVE_EXPRESSION_INVALID`
- `ADVISORY_QE_ALPHA_MVE_PIT_LEAKAGE`
- `ADVISORY_QE_ALPHA_MVE_COVERAGE_INSUFFICIENT`
- `ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID`
- `ADVISORY_QE_ALPHA_MVE_RESOURCE_LIMIT_EXCEEDED`

CLI 对 expected/unexpected error 均输出单行 typed JSON 和非零退出码。不得 broad exception 后继续成功。

## 13. Implementation plan

1. 先实现 route/request/proposal AST 合同和 exact 24 roster，完成未知字段、hash、预算和路由单测。
2. 实现只读 source loader、静态 schema seal 与向量化 allowlist evaluator，完成 future-poison 和非法 operator 测试。
3. 实现 daily metrics、block/family-wise inference、DSR 与一次性 frontier selector。
4. 实现 immutable bundle、registry、N3 route、exact retry 和薄 CLI。
5. 执行多轮正确性、PIT、统计、资源和交付审核修复；全部门禁通过后提交 PR 并合入。
6. 合入后从 WSL 原生 clean detached worktree 冻结正式 request，运行 24 proposal MVE，并回读 bundle/registry/route；不等待或要求 backend restart。

## 14. Risks and controls

| risk | control |
|---|---|
| 已知 N1/N2 结果诱导事后阈值 | 路由使用“平均每天至少召回一个全市场 Top5 winner”的结构目标，不以候选收益调阈值 |
| AST 成为动态代码执行入口 | 仅递归解释 Pydantic/JSON node，禁止 Python AST/eval/exec/import/callable |
| rolling 泄漏 T+1 | instrument/date 正序、window trailing only、future-poison 测试 |
| static revision 或 schema 漂移 | request 绑定 file hash/size/schema/required fields，运行前后 readback |
| 24 次搜索产生虚假正结果 | 固定预算、family-wise block interval、DSR、完整 frontier、一次选点 |
| 大表 row explosion/OOM | outcomes keys唯一；source merge validate；子表达式缓存与float32 score panel；16GiB RSS gate |
| 无效/停牌样本被删除或填0 | 保留 NaN/coverage reason，日评价需最低coverage，不删除整日 |
| 上游结果误入生产 | study/decision use/deployable/runtime/factor catalog literal gates全部固定为探索/noop |

## 15. Verification plan

- route 每个 evidence ref 的 hash/size/公开 schema 与唯一选择；
- exact 6×4 proposal roster、AST node/depth/field/window/operator/方向约束；
- future-poison、lag/rolling T cutoff、same-date transform、safe divide；
- source merge key、coverage、停牌/缺失保留；
- RankIC/Top5/lift/churn/block CI/Bonferroni/DSR 边界；
- frontier one-shot selection 与零 candidate；
- bundle mutation/partial/exact retry/registry/route；
- direct tests、完整 advisory modeling、ruff/format/compile/diff、L0、ownership、F2；
- 合入后从 clean WSL merge SHA 运行正式 MVE 并 inspect/readback。

## 16. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-838 | N1/N2完整证据只选择N3上游Alpha MVE，不并行下游模型线 |
| F-839 | execution request绑定preparation、Qlib/static/outcome/source/commit且sealed=false |
| F-840 | 6族×4 proposal和声明式AST exact冻结，无动态代码执行 |
| F-841 | T-visible lag/rolling/same-date evaluator与future-poison不变性 |
| F-842 | 171万行CURRENT_IC_PARENT H20成本后outcome一对一评价，无DB/network |
| F-843 | 24 proposal完整RankIC/Top5/lift/churn/correlation/coverage指标 |
| F-844 | 95% block、24-trial family-wise interval和DSR诊断 |
| F-845 | frontier一次选点，未达全部门槛时selected=0且不得调方向/预算 |
| F-846 | immutable bundle、1条registry record、N3 route和exact retry |
| F-847 | 无模型/因子库/StrategyPackage/Selection/runtime/DB/restart激活 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-838 | `qe_alpha_mve_pipeline._validate_route_sources`; `AdvisoryN3RouteReceiptV1` | `backend/tests/advisory_model_first/test_qe_alpha_mve_contracts.py` | PASS | none |
| F-839 | `prepare_qe_alpha_mve_request`; `FrozenAdvisoryQEAlphaMVERequestV1` | `backend/tests/advisory_model_first/test_qe_alpha_mve_contracts.py`; `backend/tests/advisory_model_first/test_qe_alpha_mve_delivery.py` | PASS | none |
| F-840 | `build_default_proposals`; `validate_expression`; `compile_proposal_scores` | `backend/tests/advisory_model_first/test_qe_alpha_mve_contracts.py` | PASS | none |
| F-841 | `_trailing_operation`; `compile_proposal_scores` | `backend/tests/advisory_model_first/test_qe_alpha_mve_pipeline.py` | PASS | none |
| F-842 | `_load_verified_sources`; `build_source_panel`; `_normalize_outcomes` | `backend/tests/advisory_model_first/test_qe_alpha_mve_pipeline.py`; artifact `09137f0c.../source_identity_receipt.json` | FORMAL_PASS | none |
| F-843 | `_evaluate_one_proposal_daily`; `_summarize_one_proposal` | `backend/tests/advisory_model_first/test_qe_alpha_mve_pipeline.py`; artifact `09137f0c.../proposal_summary.json` | FORMAL_PASS | none |
| F-844 | `_moving_block_interval`; `_deflated_sharpe_diagnostic` | `backend/tests/advisory_model_first/test_qe_alpha_mve_pipeline.py`; artifact `09137f0c.../proposal_summary.json` | FORMAL_PASS | none |
| F-845 | `evaluate_proposals` frontier receipt | `backend/tests/advisory_model_first/test_qe_alpha_mve_pipeline.py`; artifact `09137f0c.../frontier_receipt.json` selected=0 | FORMAL_PASS | none |
| F-846 | `_publish_bundle`; `_read_bundle`; `_deliver_bundle`; CLI | `backend/tests/advisory_model_first/test_qe_alpha_mve_delivery.py`; bundle `09137f0c...`; registry total=22；exact retry no-op | FORMAL_PASS | none |
| F-847 | literal request/receipt/manifest/route gates | `backend/tests/advisory_model_first/test_qe_alpha_mve_delivery.py`; bundle `09137f0c.../manifest.json`; route `N3_ALPHA_INFORMATION_SET_REVIEW` | FORMAL_PASS | none |

## 18. DESIGN-COMPLIANCE-001

1. 不把 24 公式生成、单窗口 IC 或 exploratory candidate 冒充完成的自动 alpha 挖掘或可交易模型。
2. 不用 0、均值填充、反向符号、旧因子或父包 score 冒充失败 proposal；所有异常 typed fail closed。
3. 不改变 H20 outcome、成本、PIT universe、父包、Selection、Entry/Exit 或仓位业务语义。
4. 不新增 restart、DDL、审批、因子库写入、自动训练或 activation 门禁。

## 19. Rollout / rollback and production gates

- `production_ddl_gate=noop`
- `backend_restart=noop`
- `runtime_activation=noop`
- `factor_catalog_write=noop`
- rollback：源代码通过普通 PR revert；正式 artifact/registry 为 append-only 研究事实，不覆盖、不删除、不冒充生产状态。
