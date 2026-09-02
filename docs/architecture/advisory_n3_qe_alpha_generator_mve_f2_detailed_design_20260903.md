# AIstock Advisory N3 QE Alpha Generator MVE F2 详细设计 v1.1

> 日期：2026-09-03
> 状态：`IMPLEMENTED_LOCAL_FULL_VERIFIED_FORMAL_PENDING`
> tier：F2
> objective contract：`ALPHA_RANKING`
> study type：`EXPLORATORY_SCREEN`
> decision use：`NAVIGATION_ONLY`
> production gates：backend restart / DDL / DML / factor catalog write / StrategyPackage write / runtime activation 均为 noop

## 1. Background / 已确认事实

1. N1 已证明当前候选流对全市场 Top5 winner 的绝对召回不足：Top20/40/50 平均召回仅 `0.8808%/1.6062%/1.7617%`，Top50 的 95% 上界约 `2.59%`，远低于预注册 `20%` 路由门槛。
2. N2-A/N2-B 已证明当前父包在已审计策略包中最强，但旧包替换不能修复全市场 winner 召回不足；Entry 无 confirmatory-positive arm，固定信息集 Exit learnability 未确认可学。
3. 已消费的 N3 固定 24 proposal、父包 overlay、腿间分歧和分钟信息集四个 frontier 均 `selected=0`。其中固定 24 proposal 是人工冻结公式，不是自动生成器；它及其同窗 overlay 已关闭，不能换名重跑、调权或后验挑选。
4. 分钟 MVE 的 candidate 在 384 个可评价日全部产生干预，但相对父包 RankIC 下降 `0.032643`、Top5 成本后 lift 下降 `315.32 bps`，说明继续在同一候选流上增加固定信息集或模型复杂度不是当前优先方向。
5. 现有 RD-Agent 具备 LLM 假设/因子生成能力，QE 具备因子目录、字段 schema、相关性和正式评价资产；但原生 RD-Agent 因子流只做名称级去重、允许生成任意 Python，并会进入完整工作区/回测/目录同步，不能直接作为本 MVE 的安全执行合同。
6. 2026-09-03 只读可行性预检确认：DEV `aistock_factor_catalog` 当前可提供 789 条目录元数据；QE 的 `evolution_researcher` 可解析到 `deepseek/deepseek-reasoner`；本机 Python 已安装 `litellm`。该预检未生成 proposal、未读取收益、未形成 research trial。
7. 本设计源码已完成本地完整实现：DEV目录READ ONLY快照两次得到同一`advqegencat_6608bb...`/789行；声明式generator、原创性过滤、固定10% overlay、累计多重检验、immutable bundle、registry/route和薄CLI均已有直接测试。Advisory完整回归`785 passed/16 skipped`，L0无阻断；尚未从合入后的clean main调用真实LLM或运行正式经济评价，因此没有candidate或研究结论。

## 2. Goal / 成功定义

交付一个固定预算、可审计、一次性的自动上游 Alpha 生成 MVE：

1. 自动生成而非人工编写一批声明式 Alpha AST；生成器只接收冻结字段、grammar、旧表达式排除清单、目录元数据和经济机制约束，不接收 IC、收益、Top5、父包表现或 sealed holdout。
2. 生成阶段最多 6 个主调用和 6 个 schema-only retry，每个信号族最终最多接受 4 个、全批最多评价 24 个表达式；失败生成计 attempt，禁止因结果不好扩预算。
3. 所有表达式经现有 allowlist AST validator/interpreter 执行，禁止动态 Python、`eval/exec/import/subprocess`、文件写入、数据库访问和网络访问。
4. 在与当前父包相同的 PIT 股票池、H20 成本后 outcome 和已消费开发窗口上，评价固定 10% rank overlay 对父包的增量 RankIC、Top5 净超额、干预支持、换手和时段稳定性。
5. 同时执行 exact/structural/目录/score-correlation 原创性检查和已知效应标注；不把已知动量、反转、规模、换手、波动、流动性、价值、质量或行业 beta 的简单复刻称为新 Alpha。
6. 一次性选择 0 或 1 个 exploratory candidate。`selected=1` 只进入独立 confirmation 设计；`selected=0` 关闭本次 daily/static grammar generator lineage，并转向新的上游数据源 MVE 设计，不再扩大 prompt、模型、调用次数或表达式预算。

## 2.1 Scope / 范围

本切片只交付一个离线 generator MVE：冻结请求与目录快照、固定 LLM 生成、target-free 原创性过滤、复用现有 AST 解释器的离线经济评价、content-addressed bundle、trial registry 与 route。源码写入严格限定为 §4 的两个新模块、一个薄 CLI、三个 direct test、CI classifier 精确映射以及本设计/蓝图；任何新增文件必须先回写本范围。运行输出只进入独立 research artifact root，不进入生产目录、模型目录或因子库。

## 3. Non-goals

- 不建设 AlphaGen/AlphaAgent/LLM-MCTS 通用平台、scheduler、UI、审批、自动提示词自修改或无限演进循环。
- 不调用 RD-Agent 原生 factor coder/runner，不执行 LLM 生成的 Python，不创建 QE workspace、MLflow run、模型或回测任务。
- 不读取或改变 sealed holdout，不把生成结果写入因子库、数据库、`rd_factors_lib`、StrategyPackage、Selection、Program descriptor 或生产运行时。
- 不重跑旧 24 proposal、旧 overlay、腿间或分钟 frontier；不反向符号、不按经济结果修 prompt、不结果后增加字段、权重、family、seed 或预算。
- 不把正 IC、LLM 解释、目录新名称、单个子窗口正收益或 hindsight 最优项作为 confirmation/activation evidence。
- 不处理历史归档、旧 artifact/root、非阻碍性平台重构或通用 ModelOps。

## 4. Architecture / 三阶段隔离

```text
Phase A: target-free preparation
  DEV factor catalog READ ONLY -> content-addressed metadata snapshot
  old 24 roster + static schema + grammar + route -> frozen generator request
                                     |
Phase B: generation only             v
  6 family prompts -> LLM JSON -> strict AST parse -> target-free novelty filters
  (network allowed; no outcome/IC; no factor code execution; no catalog write)
                                     |
  immutable generation bundle -------+
                                     v
Phase C: offline economic evaluation
  frozen Qlib/static + parent outcome + existing AST interpreter
  fixed 10% rank overlay -> metrics/stability/frontier
  (network/DB disabled; no sealed holdout)
                                     |
  immutable MVE bundle + one registry row + route page
```

新增精确源码范围：

- `backend/services/advisory_model_first/qe_alpha_generator_contracts.py`
- `backend/services/advisory_model_first/qe_alpha_generator_pipeline.py`
- `scripts/advisory_qe_alpha_generator_mve_run.py`
- `backend/tests/advisory_model_first/test_qe_alpha_generator_contracts.py`
- `backend/tests/advisory_model_first/test_qe_alpha_generator_pipeline.py`
- `backend/tests/advisory_model_first/test_qe_alpha_generator_delivery.py`
- `scripts/ci_change_classifier.py` 与对应测试中的精确 CLI 映射
- 本设计与主蓝图状态修订

允许复用但不改变语义：`qe_alpha_mve_contracts.py` 的 AST 类型/validator、`qe_alpha_mve_pipeline.py` 的 allowlist interpreter、source panel、日指标和 block inference helper。禁止复制第二套表达式解释器或评价口径。

## 5. Contracts / Phase A target-free preparation

`FrozenAdvisoryQEAlphaGeneratorRequestV1` 必须绑定：

- clean `origin/main` commit、当前 route、trial registry hash；
- 旧 QE bundle `09137f0c.../proposal_roster.json` 及 24 个 canonical AST hash；
- 父 overlay bundle `fdca2130...`、分钟 bundle `0076a3a6...` 的完成 receipt，只证明旧 lineage 已关闭，不把其经济结果放入 prompt；
- Qlib daily/static snapshot、canonical PIT universe、N2-B `CURRENT_IC_PARENT` outcome、policy 和成本 identity；
- grammar/operator/field schema 与 evaluator source hash；
- DEV 因子目录只读 metadata snapshot 的 hash/size/row count/schema。

目录快照只允许字段：`factor_name/source/catalog_version/expression/formula_hint/variables/factor_formulation/factor_type/data_source/dedup_hash/is_available`，以及 `code_text` 的 SHA256；禁止导出 `code_text` 正文、performance、IC、收益、评级、凭据或任意秘密。读取必须处于 `READ ONLY` 事务并回滚；Phase A 结束后 Phase B/C 只消费文件快照。

preparation 不调用 LLM、不加载 outcome、不计 research trial。目录行数变化只产生新 request identity，不覆盖旧 request。

## 6. Phase B / generator contract

### 6.1 Fixed budget

- signal families：`PRICE_VOLUME_BEHAVIOR`、`MONEYFLOW_BEHAVIOR`、`FUNDAMENTAL_CHANGE`、`SECTOR_RELATIVE`、`CROWDING_DISPERSION`、`REGIME_CONDITIONED`；
- 每族 1 个主调用，要求 4 个 proposal；仅当 JSON/schema/AST/字段合法性失败时允许同族 1 个 schema-only retry；
- `max_generation_calls=12`、`max_raw_generation_attempts=48`、`max_evaluated_expressions=24`、`concurrency=1`；
- retry 只能收到 machine-readable schema violation，不得收到 IC、收益、相关性或其它经济反馈；成功族不得再次调用；
- 不足 24 个合法 proposal 时允许以实际数量继续，但每族至少 2 个、全批至少 12 个，否则 typed `GENERATION_SUPPORT_INSUFFICIENT`，不进入经济评价；不得临时补人工作品。

### 6.2 LLM identity and secret boundary

- 默认 agent locator 固定 `evolution_researcher`，模型 readback 固定并写入 request；正式 v1 预期 `deepseek/deepseek-reasoner`，发生漂移则冻结新 request，不能静默接受。
- prompt template、system/user message、temperature、top_p、provider-supported seed、timeout 和 response schema 均写入 request/hash；`temperature=0`、`top_p=1`。
- API key 只从现有非秘密 locator 解析并在内存使用；artifact 只记录 locator、model、provider、request/response hash、token/latency telemetry，不保存 key、authorization header 或包含秘密的 exception。
- LLM 不保证字节确定性。因此第一次完整成功响应冻结为 content-addressed generation bundle；exact retry 复用该 bundle，不重新调用模型。

### 6.3 Output schema and code safety

每个 proposal 必须给出：`proposal_id/family/economic_hypothesis/mechanism/known_effect_exposures/source_fields/expression/direction`。`expression` 必须直接通过现有 `QEAlphaExpressionNodeV1/QEAlphaProposalV1`：

- 只允许当前已实现 operator 子集；未实现的 `TRAILING_CORR/INDUSTRY_DEMEAN` 在 v1 继续拒绝，不 silent fallback；
- AST 最大 64 nodes、深度 8、raw fields 8、lag/window 1..252；same-date 操作只在当日 canonical PIT 成员内；
- 不接受 Python/code/import/module/function/callable/path/URL；未知字段、lead、centered rolling、future label/outcome、结果字段全部 typed reject；
- direction 在生成时冻结；评价后不得整体乘 `-1`。

## 7. Originality and alpha-illusion controls

原创性过滤全部在读取 outcome 前完成，按以下顺序执行：

1. **Exact identity**：canonical AST hash 不得命中旧 24、当前批次或目录 `expression/factor_formulation/dedup_hash` 的 canonical identity。
2. **Structural identity**：按 operator multiset、source-field set、window set、tree edges 形成 fingerprint；与旧 24 或当前批次任一项 weighted Jaccard `>=0.90` 则拒绝。
3. **Field novelty**：每项至少包含 1 个旧 24 未使用字段；每族 4 项中至少 2 项包含 2 个旧 24 未使用字段。只改 window、归一化、名称或常数不满足。
4. **Known-effect declaration**：必须从固定九类 known effect roster 显式标注 1..3 项及机制差异；空标注或声称“全新/无已知效应”拒绝。该标注是归因，不是 Alpha 证据。
5. **Target-free score overlap**：在冻结 PIT panel 上计算与旧 24 score 及 parent score 的 same-date Spearman；`max_abs_old_score_corr>=0.90` 或 `abs_parent_corr>=0.80` 拒绝经济评价。该步骤不读取 label/outcome。

目录 snapshot 只能证明“未发现等价实现”，不能证明全市场原创；报告必须保留 `catalog_snapshot_scope` 和该结论边界。

## 8. PIT, data and evaluation clock

- signal dates `2024-07-04..2026-02-02`，outcome cutoff `2026-03-10`；只使用已消费开发窗口，`sealed_holdout_accessed=false`。
- source 与旧 QE MVE 一致：冻结 Qlib daily `open/high/low/close/volume/amount`、冻结 `static_factors.parquet` 和 N2-B `CURRENT_IC_PARENT/outcome_known=true/economic_net_excess_bps`。
- rolling/lag 按 instrument/date 正序且只读 `<=T`；T+1 future poison 不得改变 T score/hash。
- 停牌、未上市、合法缺失保留 NaN/reason；不得填 0、未来回填、删除整日或以部分 Top5 平均冒充完整 Top5。
- Phase C 启动前显式禁止网络和数据库；任何网络/DB access 视为 bundle invalid。

## 9. Frozen business action and metrics

每个通过 target-free 过滤的表达式只形成一个经济 trial：

```text
alpha_rank = same-date percentile(expression_score)
parent_rank = same-date percentile(parent_score)
overlay_score = 0.90 * parent_rank + 0.10 * alpha_rank
```

固定 10% 是 v1 唯一权重，不搜索 5%/20%、不按结果重标方向。每项同时报告：

- standalone RankIC（诊断）和 overlay RankIC、相对 parent RankIC delta；
- overlay Top5 H20 成本后净超额、相对 parent paired daily lift；
- Top5 intervention count/day fraction/quarter count、Top5 churn；
- finite coverage、evaluable days、parent/old-factor score correlation；
- 全窗口、前/后半段和四个连续时间块的 RankIC delta 与 Top5 lift；
- 20 日 moving-block 95% interval；
- 当前批次 24-trial interval，以及将旧 standalone 24 + 旧 overlay 24 + 本批实际经济 trial 合并的 cumulative-lineage family-wise interval；
- trial-count-aware DSR、skew、kurtosis 和 resource telemetry。

## 10. One-shot frontier and routes

proposal 只有同时满足以下预注册条件才 eligible：

1. target-free originality 全部 PASS；
2. finite row fraction `>=0.95`、evaluable days `>=382`；
3. intervention days `>=96`、intervention day fraction `>=0.25`、intervention quarter count `>=6`；
4. cumulative-lineage family-wise RankIC delta lower `>0`；
5. cumulative-lineage family-wise Top5 lift lower `>0 bps`；
6. 后半窗口 RankIC delta 与 Top5 lift point 均 `>0`；
7. 四个连续时间块至少 3 个的 RankIC delta 和 Top5 lift point 同时 `>0`；
8. 无 PIT/schema/duplicate/degenerate/resource failure。

若多个 eligible，只按 `cumulative_familywise_top5_lift_lower DESC, cumulative_familywise_rank_ic_delta_lower DESC, proposal_id ASC` 选择一次。其余 frontier 全部冻结；confirmation 失败不得回选。

- `selected=1` -> `N3_QE_ALPHA_GENERATOR_CANDIDATE_CONFIRMATION_DESIGN`
- `selected=0` 且 generation/evaluation support 充分 -> `N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN`
- generation support 不足或实现错误 -> `N3_QE_ALPHA_GENERATOR_FEASIBILITY_REPAIR`，只允许 exact schema/code/data identity 修复，不得使用经济结果扩预算或改 prompt 假设。

## 11. Artifacts, registry and exact retry

generation bundle 固定包含 request、catalog snapshot、old-roster exclusion snapshot、六族 prompt/response descriptors、accepted/rejected ledger、proposal roster、manifest。MVE bundle 固定包含 generation reference、source identity、score/overlay panel、daily metrics、proposal summary、stability report、frontier、resource report、registry record、receipt 和 manifest。

- manifest 绑定每个文件 SHA256/size/row_count 和 request/source/result identity；partial、mutation、missing 或 extra member fail closed。
- registry 追加一个 `ADVISORY-N3-QE-ALPHA-GENERATOR-MVE-V1` record，分别记录 generation calls/raw attempts/accepted/evaluated/selected、cumulative trial count、objective、study、decision use、消费窗口和 route。
- exact retry 先按 request identity 查找完整 bundle；存在则 inspect 后返回 same identity、registry duplicate-noop、route exact-noop，不调用 LLM、不重复评价。
- raw LLM response 只作为该生成 attempt 的不可变证据；不得将失败响应人工修成 proposal 后写回原 attempt。

## 12. Error contract

- `ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID`
- `ADVISORY_QE_ALPHA_GENERATOR_MODEL_IDENTITY_MISMATCH`
- `ADVISORY_QE_ALPHA_GENERATOR_LLM_CALL_FAILED`
- `ADVISORY_QE_ALPHA_GENERATOR_RESPONSE_INVALID`
- `ADVISORY_QE_ALPHA_GENERATOR_GENERATION_SUPPORT_INSUFFICIENT`
- `ADVISORY_QE_ALPHA_GENERATOR_EXPRESSION_INVALID`
- `ADVISORY_QE_ALPHA_GENERATOR_ORIGINALITY_REJECTED`
- `ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH`
- `ADVISORY_QE_ALPHA_GENERATOR_PIT_LEAKAGE`
- `ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID`
- `ADVISORY_QE_ALPHA_GENERATOR_RESOURCE_LIMIT_EXCEEDED`

CLI 对 expected/unexpected error 均输出单行 typed JSON 和非零退出码；exception 必须脱敏。失败不得发布成功 manifest、追加完成 registry 或推进 route。

## 13. Implementation plan and resources / 实施方案与资源

- concurrency=1，RSS<=16 GiB，temp<=32 GiB；wall time 只记录、不设自动停止门禁。
- 生成阶段网络串行；评价阶段复用旧 pipeline 的单份 source panel、AST 子表达式 cache 和 float32 score panel，不 materialize 24 份源表。
- 已完成：contracts/fixtures -> target-free catalog snapshot + generator -> originality -> offline evaluator/overlay -> artifacts/registry/route -> CLI/delivery tests。
- 待执行：源码经CI通过并合入后，从clean merged main冻结正式catalog/request，调用固定LLM生成并运行经济评价；不得从当前dirty/未合入源码形成正式证据。

## 14. Verification plan

- request/hash/unknown-field/model drift/secret redaction；
- READ ONLY catalog snapshot 精确字段与 transaction rollback；
- 6 family、12-call/48-attempt/24-evaluation budgets，schema-only retry 不接收经济反馈；
- AST operator/field/node/depth/window/direction 与 arbitrary-code rejection；
- exact/structural/catalog/field/known-effect/score-correlation originality；
- outcome 不可出现在 prompt/generation bundle，Phase C 网络/DB fail-closed；
- future poison、same-date canonical PIT、missing/suspend preservation；
- fixed 10% overlay、RankIC/Top5/intervention/churn/time-block/block CI/cumulative family-wise/DSR；
- one-shot frontier、selected 0/1 路由、exact retry、bundle mutation/partial/extra member；
- direct tests、完整 `advisory_modeling_backend`、ruff/format/compile/diff、L0、ownership、F2、CI verdict；
- formal generation/evaluation bundle inspect、registry/route readback，不要求 backend restart 或 DDL。

## 15. Risks and controls

| risk | control |
|---|---|
| LLM 把旧公式换名 | exact + structural + catalog + target-free score overlap 四层拒绝 |
| LLM 输出任意代码或幻觉字段 | JSON AST strict schema；现有 allowlist interpreter；未知字段/代码 typed reject |
| economic result 进入下一 prompt | Phase B 不加载 outcome；prompt manifest 逐字 hash；retry 只含 schema violation |
| 不确定 LLM 破坏 exact retry | 第一次完整响应 content-addressed 冻结；retry 复用 bundle，不重新调用 |
| 目录读取泄露性能或秘密 | 字段 allowlist、code hash only、READ ONLY 回滚、artifact secret scan |
| 24 次新搜索造成假阳性 | 当前批次和跨旧 standalone/overlay 的 cumulative family-wise + DSR |
| 单窗口短期有效冒充抗衰减 | 后半窗口和 4-block 同向门槛；结果仍只 navigation |
| 低权重 overlay 恒等 | 预注册干预次数/日期比例/季度支持门槛 |
| 继续同信息集无限搜索 | 固定一次生成预算；selected=0 转新数据源，不扩 prompt/model/trial |
| 治理膨胀 | 只新增单一离线 CLI、两个模块和三个 direct tests；无平台/UI/scheduler/DB schema |

## 16. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-197 | 旧24/overlay/腿间/分钟lineage关闭后只进入新的自动QE Alpha generator MVE |
| F-198 | 三阶段隔离：目录只读准备、LLM无target生成、DB/network-off离线经济评价 |
| F-199 | 6族固定调用/attempt/evaluation预算，schema-only retry且失败生成计attempt |
| F-200 | 只接受allowlist声明式AST，不执行LLM Python或RD-Agent coder/runner |
| F-201 | exact/structural/catalog/field/known-effect/score-correlation原创性与alpha-illusion边界 |
| F-202 | 固定10% parent rank overlay和完整增量经济/干预/稳定性指标 |
| F-203 | 当前批次加跨旧standalone/overlay累计trial的family-wise/DSR控制 |
| F-204 | 一次性frontier；selected=0转新数据源、selected=1只转独立confirmation |
| F-205 | immutable generation/MVE bundle、registry、route、secret-safe exact retry |
| F-206 | 无sealed holdout、factor/StrategyPackage/Selection/runtime/DB写入、restart或DDL |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-197 | `qe_alpha_generator_contracts.py`; `qe_alpha_generator_pipeline.py` route contract | `backend/tests/advisory_model_first/test_qe_alpha_generator_contracts.py`; `python -m nox -s advisory_modeling_backend` | PASS | none |
| F-198 | request phase gates；READ ONLY catalog snapshot；generation/evaluation split | `backend/tests/advisory_model_first/test_qe_alpha_generator_pipeline.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_qe_alpha_generator_preflight_20260903/catalog_snapshot.json` | PASS | none |
| F-199 | fixed budget fields；generation receipt support accounting | `backend/tests/advisory_model_first/test_qe_alpha_generator_contracts.py`; `backend/tests/advisory_model_first/test_qe_alpha_generator_delivery.py` | PASS | none |
| F-200 | existing `validate_expression/compile_proposal_scores`；strict generator proposal parser | `backend/tests/advisory_model_first/test_qe_alpha_generator_contracts.py`; `backend/tests/advisory_model_first/test_qe_alpha_mve_contracts.py` | PASS | none |
| F-201 | `preliminary_originality_reasons`; vectorized `target_free_score_overlap` | `backend/tests/advisory_model_first/test_qe_alpha_generator_pipeline.py` | PASS | none |
| F-202 | `evaluate_generated_overlays` fixed rank overlay | `backend/tests/advisory_model_first/test_qe_alpha_generator_pipeline.py` incremental/unknown-Top5 cases | PASS | none |
| F-203 | `_summarize_overlay` current+cumulative block inference/DSR | `backend/tests/advisory_model_first/test_qe_alpha_generator_pipeline.py` | PASS | none |
| F-204 | frontier/receipt exact 0/1 route relation | `backend/tests/advisory_model_first/test_qe_alpha_generator_contracts.py`; `backend/tests/advisory_model_first/test_qe_alpha_generator_pipeline.py` | PASS | none |
| F-205 | generation/result publisher/reader、exact retry、registry/route、CLI | `backend/tests/advisory_model_first/test_qe_alpha_generator_delivery.py` | PASS | none |
| F-206 | literal no-write/no-activation fields、secret redaction、L0 | `backend/tests/advisory_model_first/test_qe_alpha_generator_delivery.py`; `python -m nox -s l0` | PASS | none |

## 18. DESIGN-COMPLIANCE-001

1. 本设计交付的是可运行的自动生成 MVE，不把人工公式、prompt 文档、固定 fixture 或原生 RD-Agent 完整 loop 冒充生成器。
2. 不以名称变化、window 微调、符号反转、silent fallback、人工作品补位或 partial Top5 形成伪 candidate。
3. 不改变当前策略包、Selection、H20 outcome、成本、PIT universe、review policy、Entry/Exit、仓位或页面业务语义。
4. 不新增未授权 restart/DDL/DML/激活；正式结果只有在源码合入、clean-main运行、bundle inspect、registry/route readback后才能报告。

## 19. Rollout / rollback and production gates / 发布、回滚与生产门禁

- `production_ddl_gate=noop`
- `backend_restart=noop`
- `runtime_activation=noop`
- `factor_catalog_write=noop`
- rollback：源码通过普通 PR revert；content-addressed generation/MVE artifact 与 append-only registry 保留为研究事实，不覆盖、不删除、不进入运行时。
