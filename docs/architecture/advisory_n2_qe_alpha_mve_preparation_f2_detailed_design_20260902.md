# AIstock Advisory N2 QE 上游 Alpha MVE 无证据准备 F2 详细设计 v1.0

- 日期：2026-09-02
- Feature tier：F2
- 当前状态：`DESIGN_READY_IMPLEMENTATION_PENDING`
- 业务归属：Selection Center / Advisory / Model-first Research / QE
- 蓝图条目：F-186、§6.9、§9 N2→N3 边界
- 目标合同：未来实验固定为 `ALPHA_RANKING`
- 当前交付性质：`PREPARATION_ONLY_NO_RESEARCH_EVIDENCE`

## 1. 背景与已知事实

1. N1 已证明全市场赢家 Top20/40/50 召回很低；Top20 内 clairvoyant 空间高，但当前固定信息集 Ridge learnability 不足以确认。该事实支持准备上游 alpha 路线，不等于 N3 已正式选择该路线。
2. N2-A 已审计当前父包两腿和 IC 组合；N2-B 独立 StrategyPackage 审计与 Entry/Exit formal audit 仍是 N2 的独立诊断。准备件不得读取这些运行中的经济结果，也不得据此调整预算或语法。
3. AIstock 已有 QE formal dataset contract、canonical PIT、Qlib daily/suspend 固定 pins、QE factor code/cache 和 trial registry；本任务复用这些资产，不建设新数据平台、因子平台或审批系统。
4. 当前 QE/RD-Agent 生成的是 Python factor code，而不是已经存在的统一公式 DSL。故本任务冻结的是未来生成器必须消费的**抽象 grammar/operator policy**，不在本轮实现代码生成、代码执行或 DSL 编译器。
5. 准备件不产生 candidate、factor code、IC、RankIC、收益、相关性、SHAP、衰减或 winner；也不向 trial registry 追加记录。

## 2. Scope / 范围与目标

交付一个最小、不可变、可机器校验的 QE alpha MVE preparation contract：

1. 精确绑定现有 QE dataset contract 和 Qlib/suspend/PIT pins；禁止数据库、网络和滚动 latest 数据源。
2. 冻结抽象表达式 grammar、允许 operator、显式 future/leakage/security 禁止项和窗口边界。
3. 冻结未来 MVE 的提案预算、信号族配额、资源上限、并发和 trial 计数规则；当前执行预算全部为 0。
4. 冻结原创性、已知效应重叠、时间衰减、经济归因和种子稳定性在未来 N3 实验中的必报项；不在准备阶段读取这些指标。
5. 冻结 future registry lineage template、objective/study/decision-use 和 sealed holdout 边界，但 preparation 本身不登记为 trial。
6. 提供 `build / write / inspect` 薄 CLI；只能生成或校验 preparation JSON，不能调用 QE、RD-Agent、LLM、因子执行器或评价器。

## 3. Non-goals

- 不生成、编译、执行、筛选、评级、保存或入库任何 factor/candidate。
- 不读取 IC、RankIC、收益、换手、相关矩阵、SHAP、衰减、回测或模型输出。
- 不调用 `qe_evolution_service`、`qe_evolution_agents`、RD-Agent、LLM、factor cache compute 或 official evaluation。
- 不创建 StrategyPackage，不改变现有包/因子生命周期，不触碰 Selection/Advisory runtime。
- 不访问 sealed holdout，不追加 trial registry，不生成 direction/activation evidence。
- 不建立通用 DSL、搜索平台、prompt 平台、UI、scheduler、数据库表、DDL/DML、RBAC 或人工审批。
- 不执行后端重启、依赖安装、数据集更新或 production activation。

## 4. 架构

```text
existing QE dataset constants + N0/N1 research-control identities
                              |
                    deterministic builder
                              |
        FrozenAdvisoryQEAlphaMVEPreparationV1
          /          |           |           \
 data identity   grammar     budget/resource  future lineage
          \          |           |           /
             immutable preparation JSON
                              |
                  inspect / exact retry only
                              |
 generation=false, evaluation=false, registry_append=false
```

代码范围：

- `backend/services/advisory_model_first/qe_alpha_mve_preparation.py`：typed contract、默认 builder、immutable write/read、source-constant parity。
- `scripts/advisory_qe_alpha_mve_prepare.py`：`build/inspect` CLI；不含业务计算。
- `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py`：身份、预算、禁止项、无证据边界和 exact retry。
- `scripts/ci_change_classifier.py` 与 file ownership：仅登记新增 CLI 的既有 Advisory modeling lane。

## 5. Contracts / Data identity

准备件必须从 `backend.services.quantevolver.qe_dataset_contract` 读取并冻结：

- `QE_DATASET_CONTRACT_ID`、signal start/end；
- `QE_FROZEN_BIN_SNAPSHOT_ID`、universe key；
- instruments/calendar/meta-export SHA256；
- frozen universe fingerprint；
- suspend dataset id、parquet/manifest SHA256、source contract；
- `canonical_pit_required=true`、`immutable_release_required=true`；
- `database_read_allowed=false`、`network_read_allowed=false`、`rolling_latest_allowed=false`。

contract 自身不接收路径或环境变量作为 identity。未来 N3 request 必须另行绑定当时已部署的 exact release manifest；若上述源码 pins 已变化，旧 preparation 仍保持旧 identity，新路线必须创建新 preparation version/lineage，不得原地改写。

## 6. Expression grammar / operator policy

抽象 grammar 版本固定为 `advisory_qe_alpha_expression_grammar_v1`。允许的输入族只描述能力，不授权当前读取：

- `PRICE_VOLUME_DAILY`
- `MONEYFLOW_DAILY`
- `FUNDAMENTAL_PIT`
- `SECTOR_PIT`
- `MARKET_REGIME_T_VISIBLE`

允许 operator 名单：

- 基础：`ADD`、`SUBTRACT`、`MULTIPLY`、`SAFE_DIVIDE`；
- 有界变换：`ABS`、`SIGN`、`LOG1P_ABS`、`SQRT_ABS`、`CLIP`；
- 严格历史时序：`LAG`、`DELTA`、`TRAILING_SUM/MEAN/STD/MIN/MAX/CORR`；
- 同日横截面：`SAME_DATE_RANK`、`SAME_DATE_ZSCORE`、`INDUSTRY_DEMEAN`。

固定参数边界：

- lag `1..252`；trailing window `2..252`；`center=false`；
- 时间 rolling 只能按 instrument 向后；横截面只能在同一 decision date；
- 分母必须使用 `SAFE_DIVIDE`；非有限值保留 typed missing，禁止 future fill；
- 最大 AST 节点 64、最大嵌套深度 8、单表达式最多 8 个原始字段。

显式禁止项：

- negative shift/lead、centered rolling、time-axis backfill、反向切片后 fill；
- future label/outcome/IC/return/selection result/evaluation artifact 作为输入；
- target/label/date cutoff 之后的数据；
- 动态 import/eval/exec/compile、文件写入、数据库、网络、subprocess、shell、pickle load；
- 静默 fallback、自动 operator 替换、结果后 prompt/grammar/budget 修改。

本轮不实现编译器。未来 generator adapter 必须先证明对该 policy 的完备 enforcement，才能在新的 N3 PR 中启用。

## 7. Budget and resource contract

未来上游 MVE 的准备预算固定为：

- proposal signal families：`PRICE_VOLUME_BEHAVIOR`、`MONEYFLOW_BEHAVIOR`、`FUNDAMENTAL_CHANGE`、`SECTOR_RELATIVE`、`CROWDING_DISPERSION`、`REGIME_CONDITIONED`；
- 每族最多 4 个 proposal，总 proposal budget 24；
- 每个 proposal 的 exact semantic variant 计 1 trial；编译失败、PIT失败、退化输出也计生成尝试但不得形成经济结果；
- 当前 preparation 的 generated/evaluated/selected trial count 均为 0；
- 当前 `generation_authorized=false`、`execution_authorized=false`、`economic_evaluation_authorized=false`；
- future execution concurrency=1，RSS=16 GiB，temp=32 GiB；wall time 只记录，不设自动 stop；
- 禁止因结果不足增加 proposal、signal family、窗口、seed 或重写 prompt。

24 是一次 MVE 的上限而非必须消费数；N3 若不选择上游路线，预算保持未使用。N3 若认为该预算/信号族不合适，应创建新版本并在任何生成前评审，不能修改本 preparation 后继续使用同 identity。

## 8. Originality, decay and attribution obligations

准备件只冻结未来必报字段，不读取实际值：

1. exact code/formula canonical hash 与 factor catalog identity 去重；
2. 与当前 factor values、Selection legs 和已知效应 exposure 的相关/重叠报告；
3. 逐种子和种子平均的相关结构、LOO 边际增量；
4. 时间子窗、季度和 regime 稳定性、衰减曲线与成本后结果；
5. SHAP/经济方向归因，说明是否只是复刻 momentum、reversal、size、turnover、volatility、liquidity、value、quality 或 sector beta；
6. 任何正结果仍是 exploratory，不能直接 activation。

这些字段不预设统一“一票否决”阈值；未来研究阶段输出完整 frontier，candidate/confirmation/activation 另按各自合同选点。exact duplicate、PIT违规和未来数据使用仍是立即失败，不属于 frontier trade-off。

## 9. Future registry lineage template

preparation 包含但不提交以下模板：

- hypothesis family：`ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1`；
- parent lineage：N1 oracle/learnability、N2-A、N2-B v2、N2 Entry/Exit diagnostics；
- objective：`ALPHA_RANKING`；
- future study type：`EXPLORATORY_SCREEN`；
- decision use：`NAVIGATION_ONLY`；
- sealed holdout：false；
- planned trial count：24；generated/evaluated/selected 由未来实际 request/receipt 记录；
- result-driven retry 禁止；仅同 candidate 的代码/数据 identity 修复允许 exact retry。

preparation JSON 不含 evidence ref，不是 `AdvisoryResearchTrialRecordV1`，不得写入 registry。未来只有 route 明确为上游 QE 主线的新 N3 request 才可实例化该模板。

## 10. Immutable artifact and CLI

`build`：

- 从源码常量构造完整 contract；
- preparation hash 排除自身 id/hash/created_at，故同源码版本跨时间构建保持同 semantic identity；
- output 路径不存在时原子写入；存在且 identity 相同则 exact retry no-op；不同则 fail closed；
- 输出摘要只含 id/hash、预算、授权 false 和路径。

`inspect`：

- 验证 unknown field、self hash、source pins、operator/forbidden roster、预算、lineage 和 false authorization；
- 不导入或调用 QE/RD-Agent executor；
- 不读取任何 market/factor/economic artifact。

## 11. Error contract

- `ADVISORY_QE_ALPHA_PREPARATION_INVALID`
- `ADVISORY_QE_ALPHA_PREPARATION_CONFLICT`
- `ADVISORY_QE_ALPHA_GENERATION_NOT_AUTHORIZED`
- `ADVISORY_QE_ALPHA_EVALUATION_NOT_AUTHORIZED`
- `ADVISORY_QE_ALPHA_REGISTRY_APPEND_NOT_AUTHORIZED`

unknown field、identity drift、额外 operator、缺少 forbidden item、预算漂移、任一授权为 true 均 fail closed。

## 12. Implementation plan

1. 实现 frozen Pydantic contracts、完整 source-constant projection 和 deterministic identity。
2. 实现 default builder、immutable writer/reader、exact retry 与 typed conflict。
3. 实现仅含 `build/inspect` 的 CLI，并登记现有 Advisory modeling CI/ownership lane。
4. 增加直接边界、mutation、no-execution/import 和 CLI tests。
5. 执行多轮设计/代码审核与修复、完整 Advisory nox、L0、F2 validator、PR/CI/合入。
6. 合入后只生成 preparation JSON；不启动 generator、QE、模型训练或评价。

## 13. Risks and controls

| Risk | Control |
|---|---|
| preparation 被误当已选择 QE 主线 | schema 固定 preparation-only，所有授权 literal false，不写 registry |
| 抽象 operator 名称与未来执行器不一致 | 本轮不实现 adapter；未来 N3 PR 必须证明完备 mapping 后才能执行 |
| 24 个 proposal 形成结果后扩搜 | budget/6×4 family 配额进入 semantic identity；修改必须新版本且发生在生成前 |
| future/leakage operator 绕过 | exact denylist + 未来 adapter 完备性测试；PIT错误不进入 frontier |
| 复制造成 alpha illusion | 原创性、已知效应重叠、衰减、SHAP/经济归因列为未来必报项 |
| 治理平台化 | 单 contract + 单薄 CLI + 单测试文件；无 UI/DB/scheduler/approval |

## 14. Verification plan

- semantic hash 稳定，created_at 不改变 identity；
- 当前 QE source constants 全量映射，任一 pin 漂移拒绝；
- operator roster、signal family、窗口/AST/字段边界 exact；
- negative shift、centered rolling、backfill、future label、DB/network/subprocess 等禁止项不可删除；
- 24 proposal、6×4、0 当前 trial、单并发、16/32 GiB、无 wall stop exact；
- generation/execution/evaluation/registry append 恒为 false 且不能 override；
- preparation 无 evidence ref、IC、收益、相关、SHAP、winner 字段；
- immutable write、exact retry、mutation/unknown field 失败；
- CLI 单 JSON、失败非零；
- direct tests、ruff/format/compile/diff、`advisory_modeling_backend`、L0、F2 validator。

## 15. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-186 | N3 正式分流前只允许无证据 preparation，禁止生成/评价/结果驱动调整 |
| F-820 | 复用现有 QE dataset/Qlib/suspend/PIT 常量并冻结完整 data identity；无 DB/network/latest |
| F-821 | grammar/operator allowlist 与 future/leakage/security denylist 精确、不可结果后修改 |
| F-822 | 6 个 signal family×4、24 proposal、资源/并发/no-wall-stop 和 trial 计数规则冻结 |
| F-823 | 当前 generation/execution/evaluation/registry append 全部禁止且不能 override |
| F-824 | 原创性、已知效应重叠、衰减、种子、LOO、成本与经济归因作为未来必报义务 |
| F-825 | future registry lineage template 固定但 preparation 本身不是 trial/evidence |
| F-826 | immutable build/inspect/exact retry；mutation、unknown field、source drift fail closed |
| F-827 | 无 QE/RD-Agent/LLM/market/economic execution，无 API/DB/runtime/DDL/restart |

## 16. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-186 | `FrozenAdvisoryQEAlphaMVEPreparationV1` preparation-only root contract | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py`；`python -m nox -s advisory_modeling_backend` | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-820 | `QEAlphaDataIdentityV1` 与当前 QE source pins exact projection | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` source parity/drift cases | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-821 | `QEAlphaExpressionPolicyV1` grammar/operator/forbidden rosters | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` mutation cases | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-822 | `QEAlphaBudgetV1` 6×4、资源与 0 current trial contract | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` budget/resource case | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-823 | root literal-false authorization fields；`require_preparation_operation` | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` operation denial/bypass-object cases | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-824 | `QEAlphaFutureEvidenceObligationsV1` | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` exact roster case | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-825 | `QEAlphaFutureLineageTemplateV1`；root 不含 evidence refs | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` no-evidence case | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-826 | immutable writer/reader；`scripts/advisory_qe_alpha_mve_prepare.py` | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py` exact retry/conflict/CLI cases | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |
| F-827 | strict import/call/write boundary；CI/ownership mapping | `backend/tests/advisory_model_first/test_qe_alpha_preparation_boundary.py`；`backend/tests/scripts/test_ci_change_classifier.py`；`backend/tests/test_validation_catalog_integrity.py`；F2 validator 9/9 | IMPLEMENTED_LOCAL_FULL_VERIFIED | none |

本地验证 receipt（2026-09-02）：direct boundary `18 passed`；CI classifier/catalog `80 passed`；`advisory_modeling_backend` `691 passed, 16 skipped`；ruff、format、compile、diff check 全部通过；F2 validator `9/9`。这些结果只验证准备件源码，不构成 QE proposal、经济结果或主线选择证据。

## 17. DESIGN-COMPLIANCE-001

1. preparation 不冒充 factor、模型、实验、回放、formal result 或可激活能力。
2. 无 silent fallback；identity、grammar、预算、授权和文件冲突均 typed fail closed。
3. contract/identity 在 service，CLI 只 build/inspect；测试和文档不实现业务逻辑。
4. 不新增人工审批、RBAC、动态仓位、下单、数据库、runtime 或结果后放宽门禁。

## 18. Production gates and rollback

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- backend restart：noop
- DEV/production DDL/DML：无
- runtime/Selection/StrategyPackage/Factor 状态：无变化
- rollback：回退 preparation source/CLI；已生成的 preparation JSON 只是无证据冻结配置，不得冒充实验结果。
