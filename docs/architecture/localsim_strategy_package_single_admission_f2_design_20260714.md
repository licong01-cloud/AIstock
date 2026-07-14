# LocalSIM StrategyPackage 单次准入与双类型运行 F2 设计

设计日期：2026-07-14  
层级：T3 / F2  
模块：StrategyPackage、Selection Center、Paper Trading v2 LocalSIM、MiniQMT SIM、QMT strategy ledger  
阶段：用户已明确业务边界，按本设计实施  
前置实现：BUG-611 / PR #1929 `centralize StrategyPackage asset admission`、multi-alpha signal admission、multi-alpha parent self-contained runtime

## Background / 背景

StrategyPackage 已具备构建期 frozen runtime self-check 和持久化的 `runtime_asset_admission` receipt。PR #1929 已把 StrategyPackage 完整性检查集中到包进入平台的入口，并从 Selection Center 主路径和 Paper portfolio 创建路径删除了部分重复检查。

定向代码审核仍发现残留：

- `StrategyPackageRuntime.build_signal_snapshot_with_trace()` 每次生成信号仍调用 `StrategyPackageValidator.validate_manifest()`。
- LocalSIM `PaperTradingDayRunner.run_day()`、`PaperTradingReadinessService` 和 live session `_prepare_live_run()` 每次运行仍调用 `validate_manifest_identity_for_paper_trading()`，该方法实际会重算 canonical manifest hash 并重新检查 `asset_checks`。
- 低层 `PaperTradingV2Runner` 的单笔、行情单笔和批量执行入口仍重复调用 `validate_for_paper_trading()`。
- `QmtStrategyPackageBindingService.bind_with_result()` 仍调用完整 `StrategyPackageAssetEligibilityService.require_eligible()`。

这些残留与用户确认的业务边界不一致：策略包完整性只能在新包进入平台时完整检查一次；后续选股、荐股、LocalSIM 和 MiniQMT SIM 只读取已冻结身份/准入结果，并检查本次运行真正需要的数据源、交易日、runtime profile、execution policy、账户、slot、订单和账本等动态组件。

本设计同时补齐 LocalSIM 对新增 `SINGLE_ALPHA` 与 `MULTI_ALPHA` 的直接运行验收。历史缺少准入 receipt 的单 Alpha 包不做回填、不做数据修复，可按现有生命周期淘汰；不能为了兼容历史包在运行时恢复二次完整性校验或静默 fallback。

## Scope / 范围

1. 固化新 `SINGLE_ALPHA` 与 `MULTI_ALPHA` 的唯一完整性准入边界和 receipt 契约。
2. 删除 Selection/信号、LocalSIM、MiniQMT SIM/QMT binding 中残留的 StrategyPackage 完整性二次校验。
3. 保留并明确运行期必要校验，不改变信号、topK、目标权重、订单、价格、成交、现金和持仓语义。
4. 增加双类型 LocalSIM 完整日运行测试，覆盖 signal -> target -> order intent -> minute execution -> fill -> cash/position snapshot 的真实服务链。
5. 增加禁止二次校验的直接测试和静态源代码守卫。
6. 更新本设计的 Design Acceptance Matrix，并执行 F2 validator 与 DESIGN-COMPLIANCE-001。

## Non-goals / 边界

1. 不修复、不回填、不重新冻结历史缺 receipt 的 `SINGLE_ALPHA` 包；历史包可按生命周期退役。
2. 不改变任何策略信号、选股、排名、topK、目标权重、调仓或分钟执行算法。
3. 不删除运行期数据源、交易日、停牌/涨跌停、execution policy、runtime profile、MiniQMT account/group/slot、订单身份、幂等和账本一致性检查。
4. 不改变 MiniQMT broker adapter、下单、撤单、成交回报或账户查询语义。
5. 不新增 approval、RBAC、人工 acknowledge、confirmation token 或其它人工门禁。
6. 不执行 DDL/DML，不写生产配置，不调用 broker，不启动、停止或重启服务。
7. 双类型 LocalSIM 完成证据必须包含真实服务链的运行、订单、成交和账本 oracle；隔离替身、源代码守卫或单纯 portfolio creation 只能作为补充证据。

## Architecture / 架构

### 1. 唯一 StrategyPackage 完整性准入边界

新包只能通过以下权威 writer 进入平台：

- `StrategyPackageService.create_from_qe_experiment()`
- `StrategyPackageService.create_from_qe_evolution_loop()`
- `StrategyPackageService.create_from_candidate()`
- `MultiAlphaPackagePromotionService.promote()`

共同顺序必须保持：

1. 构建 manifest。
2. freeze package-owned factor/model/runtime assets。
3. `StrategyPackageValidator.validate_manifest()`。
4. `FrozenRuntimeSelfCheckService.assert_manifest_self_contained()` 完整检查一次。
5. `attach_runtime_asset_admission()` 写入 receipt。
6. 重新 freeze canonical manifest 并再次做 build-time manifest validation。
7. 原子保存 manifest 与 assets；任何失败不得保存 half package。

`runtime_asset_admission` receipt 的权威身份至少包含：

- `schema_version=strategy_package_runtime_asset_admission_v1`
- `passed=true`
- `persisted_for_simulation_admission=true`
- `package_id`
- `alpha_mode`
- `self_check_manifest_sha256`
- `asset_closure_sha256`
- `model_code_contract`
- `self_check_summary`

### 2. 模型代码条件契约

完整性准入必须按模型资产声明判断，不得把“存在模型代码文件”写成所有模型的统一门禁：

- `model_code_required=false`：允许 `model_code_assets=[]`，receipt 明确记录该契约。
- `model_code_required=true`：必须冻结并校验声明的代码 assets、相对路径和 sha；缺失必须在包入口 fail-loud。
- `MULTI_ALPHA`：父包对每个内联 leg/model 分别执行同一条件契约；运行期不得读取 legacy child package 补资产。

### 3. 下游信任边界

新包通过入口并持久化后，下游不得再次执行以下 StrategyPackage 完整性动作：

- `StrategyPackageValidator.validate_manifest()`
- `validate_manifest_identity_for_paper_trading()` / `validate_for_paper_trading()`
- `StrategyPackageAssetEligibilityService.require_eligible()`
- `FrozenRuntimeSelfCheckService.assert_manifest_self_contained()`
- model-code discovery、workspace materialization preflight、model probe 或 WSL probe

适用下游：

- Selection Center / `StrategyPackageSelectionService`
- `StrategyPackageRuntime` 信号快照
- Paper v2 portfolio create、readiness、LocalSIM day runner、live session、低层 runner
- MiniQMT SIM auto/manual runtime 与 QMT strategy binding
- 复用 Selection 服务的荐股/Advisory 路径

下游可以读取 immutable identifiers 用于关联和查找，但不得把读取变成第二次包完整性判定。`asset_eligibility` API 可继续作为 StrategyPackage Center 的入口诊断/readback，不得被运行热路径重新调用。

### 4. 运行期必须保留的校验

以下检查属于本次运行的动态事实或交易账本不变量，不是 StrategyPackage 二次准入，必须保留且 fail-loud：

| 类别 | 保留检查 |
|---|---|
| Portfolio snapshot identity | `portfolio.package_id/manifest_sha256` 与其 frozen snapshot 精确相等 |
| Selection artifact identity | artifact 的 `package_id/manifest_sha256/trade_date/data_source/runtime_config_hash` 精确命中 |
| Signal authority | single/multi 对应的 authoritative source type、scope、非空/自然空证据 |
| Data readiness | trading calendar、minute bars、suspend_d、stk_limit、pre_close、所需日特征 |
| Runtime profile | profile binding、trade-enabled、topK、HMM/ST PIT 等本次运行配置 |
| Execution policy | validated policy identity/hash、algo registry/config、运行资产 |
| MiniQMT | broker/data source、SIM mode、account/group/slot、position/cash authority、adapter fail-fast |
| Ledger | duplicate run、order/portfolio/package identity、terminal order、fill/cash/position idempotency |

不得捕获这些错误后继续，不得使用默认价格、默认现金、默认持仓、daily-mode fallback 或伪造成功。

### 5. 生命周期边界

`RETIRED` 表示不允许建立新的运行关系，但不是资产完整性二次校验：

- selectable/list source 在进入下游前排除 retired package。
- QMT 新 binding 保留精确的 `package_status == RETIRED` 生命周期拒绝，不调用 asset eligibility 或 manifest validator。
- 既有运行记录、审计和账本不因退役被删除。

### 6. 双类型 LocalSIM 执行一致性

LocalSIM 执行层只消费 broker-neutral selection artifact：

- `SINGLE_ALPHA` authoritative source type：`strategy_package_live_inference_v2`。
- `MULTI_ALPHA` authoritative source type：`live_multi_alpha_inference_v1`。
- 两者进入相同的 `TargetPositionEngine`、`RebalanceEngine`、OMS、minute execution、fill 和 ledger 链路。
- `MULTI_ALPHA` 运行只读取父包 frozen assets 和父包 artifact；不得访问 legacy child package。

## Contracts / 契约

### Runtime no-revalidation contract

运行路径的构造器可暂时保留 `validator` / `asset_eligibility_service` 注入参数作为调用兼容 seam，但运行方法不得调用其 StrategyPackage 完整性接口。测试必须注入会抛错的 sentinel，证明运行仍走通；不能用 `None` 或宽松 mock 掩盖调用。

### Error contract

- 包入口完整性失败：保持现有 typed `PackageAssetInvalidError` / `StrategyPackageValidationError` 和结构化 context。
- QMT 对 retired package 的新 binding：loud `StrategyPackageValidationError`，reason code 为 `strategy_package_retired_for_new_qmt_binding`。
- 运行期动态组件失败：保持所属模块现有 typed error/reason code，不转换为包 admission 失败。
- 任何异常不得被降级为 warning 后继续写 run/fill/ledger，除非既有设计明确该状态为等待态。

### Compatibility contract

- 新 `SINGLE_ALPHA` 和新 `MULTI_ALPHA` 由同一 receipt schema 标识一次性准入。
- 历史无 receipt 的单 Alpha 不 backfill；不承诺其新建 LocalSIM 运行。
- 现有 constructor 参数、API payload、DB schema 和响应字段保持兼容。
- 不新增状态、审批或 operator 操作。

## Design Acceptance Index / 设计验收索引

| ID | 设计项 |
|---|---|
| F-001 | 新 SINGLE_ALPHA 与 MULTI_ALPHA 均在唯一包入口执行完整 self-check 并持久化 receipt |
| F-002 | `model_code_required=true/false` 两类模型均按声明正确准入，不要求无代码模型提供代码文件 |
| F-003 | Selection/荐股运行不调用包 validator、asset eligibility、self-check 或 model/workspace/WSL preflight |
| F-004 | StrategyPackageRuntime 信号生成不重复 validate manifest，仍严格校验 authoritative artifact |
| F-005 | LocalSIM create/readiness/day/live/low-level runner 不重复校验 StrategyPackage 完整性 |
| F-006 | MiniQMT/QMT binding 不调用 asset eligibility；retired 生命周期拒绝和执行层动态门保持 |
| F-007 | SINGLE_ALPHA 与 MULTI_ALPHA 均能完成 LocalSIM 全日账本链路并产出真实 run/order/fill/cash/snapshot evidence |
| F-008 | MULTI_ALPHA 只使用父包与父包 artifact，不读取 child package |
| F-009 | 运行期数据源、交易日、profile、policy、账户/slot、订单和账本校验零回归 |
| F-010 | 历史无 receipt 的 single-alpha 不回填、不恢复运行期二次检查、不做默认 fallback |
| F-011 | 无静默错误、假成功、业务语义漂移或新增审批/门禁 |
| F-012 | 无 DDL/DML、依赖、生产配置、broker 调用或服务重启 |

## Implementation Plan / 实施方案

### Phase 1：删除残留二次校验

1. `strategy_package/runtime.py` 删除 signal snapshot 的 `validate_manifest()` 调用，保留 frozen hash presence 与 artifact identity/authority checks。
2. `paper_trading_v2/day_runner.py`、`readiness.py`、`live_session.py` 删除 `validate_manifest_identity_for_paper_trading()` 调用。
3. `paper_trading_v2/runner.py` 删除三处 `validate_for_paper_trading()` 调用，保留 intent identity 与执行组件检查。
4. `qmt_strategy_ledger/package_binding.py` 删除 asset eligibility 构造与调用；改为只保留 retired lifecycle rejection。

### Phase 2：双类型与 no-revalidation 直接测试

1. 注入 raising validator，证明 StrategyPackageRuntime 对 admitted single/multi 只消费 authoritative artifact。
2. 注入 raising validator/asset service，证明 LocalSIM day/runner 与 QMT binding 不调用二次包校验。
3. 参数化运行 admitted SINGLE_ALPHA 与 self-contained MULTI_ALPHA 父包的 LocalSIM 全日链路。
4. 对 multi 父包注入 child-failing repository，证明没有 child runtime lookup。
5. 保留并运行 model-code required/optional、admission closure drift、unknown multi blocker、MiniQMT execution gate 回归。

### Phase 3：验证与交付

1. 运行直接 nodeid、changed-file lint/compile、相关小矩阵与 `git diff --check`。
2. 运行 F2 validator 和 DESIGN-COMPLIANCE-001 矩阵。
3. 创建 PR，使用 CI/Validation Center 完成 broad Paper v2 / strategy package / qmt ledger 回归。
4. CI 全绿后合入并执行 aftercare；生产门禁全部 noop。

## Verification Plan / 验证方案

### Direct tests

- 新增双类型 LocalSIM day-run acceptance：每种类型断言 `SUCCEEDED`、真实 order/fill、cash ledger、position/account snapshot 和完整 run events。
- raising sentinel 断言所有列出的 downstream 路径未调用 StrategyPackage validator/asset eligibility。
- multi parent child-lookup sentinel 断言运行不读取 legacy child package。
- QMT retired binding 继续 loud fail，non-retired binding 在 raising asset service 下成功。

### Regression matrix

- StrategyPackage one-time admission、model-code required/optional、closure drift。
- multi-alpha signal admission、parent promotion、selection artifact authority。
- Paper v2 portfolio/day runner/readiness/live session/runner。
- QMT strategy package binding。
- MiniQMT account/slot/data-source/execution-policy blockers。

### Static and workflow gates

```powershell
rtk python -m ruff check <changed-python-files>
rtk python -m py_compile <changed-python-files>
rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/localsim_strategy_package_single_admission_f2_design_20260714.md --tier F2
rtk git diff --check
```

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `strategy_package/service.py`; `multi_alpha_promotion.py`; `frozen_runtime_self_check.py` | `test_runtime_package_assets_batch2.py` receipt tests；multi promotion receipt tests | verified | 无 |
| F-002 | `frozen_runtime_self_check.py::_model_code_contract` | `test_runtime_asset_admission_supports_models_without_model_code`；`test_runtime_asset_admission_supports_models_with_model_code` | verified | 无 |
| F-003 | `selection_center/service.py`; `simulation_runtime/selection.py` | Selection health no-revalidation test；8-path forbidden-call source guard | verified | 无 |
| F-004 | `strategy_package/runtime.py` | dual-type full-day test injects raising `StrategyPackageValidator` and consumes authoritative artifacts | verified | 无 |
| F-005 | `paper_trading_v2/day_runner.py`; `readiness.py`; `live_session.py`; `runner.py` | dual day-run、readiness、live prepare、low-level runner raising-sentinel tests | verified | 无 |
| F-006 | `qmt_strategy_ledger/package_binding.py` | non-retired bind with raising asset service；retired reason-code assertion；MiniQMT missing-account regression | verified | 无 |
| F-007 | Paper v2 LocalSIM real service chain with in-memory persistence | both alpha modes produce `SUCCEEDED` run、orders、fills、cash ledger、snapshot and complete run events | verified | 无 |
| F-008 | self-contained multi parent runtime and artifact identity | `ChildFailingRepository` proves no `pkg_mac*` child lookup | verified | 无 |
| F-009 | unchanged runtime validators and targeted regression | related matrix `67 passed`；readiness/live direct nodeids pass | verified | 无 |
| F-010 | no migration/backfill implementation | changed-file scope contains no migrations、backfill or asset mutation | verified | 无 |
| F-011 | changed code、tests and this matrix | DESIGN-COMPLIANCE-001 four-item review recorded below | verified | 无 |
| F-012 | no migrations/dependency/config/service commands | production gates all noop；broker/restart not performed | verified | 无 |

## Implementation Evidence / 实现证据

- 双类型 LocalSIM 全日链：`2 passed`。两种 alpha mode 均使用 authoritative frozen artifact、真实 target/rebalance/OMS/minute execution 与 in-memory persistent ledger；validator/asset checker 为调用即失败 sentinel。
- no-revalidation、QMT lifecycle、runner 与 source guard 直接矩阵：`13 passed`。
- readiness 与 live-run preparation sentinel：各自直接 nodeid 通过。
- StrategyPackage admission、multi-alpha、Paper runner 与 QMT binding 相关小矩阵：`67 passed`。
- changed Python files：Ruff PASS、`py_compile` PASS；`git diff --check` PASS。
- F2 workflow validator：PASS，`design_items=12`、`matrix_rows=12`、`warnings=0`。
- Validation Center deterministic receipt：
  - L0 `valjob_20260714_102036_5b17a0d0` PASS。
  - `simulation_core_l2` `valjob_20260714_102035_3d842fa6` 为 `260 passed / 7 failed`；在未修改的 canonical main 上精确重跑同一 7 个 nodeid，结果同为 7/7 failed，证明是当前 main 的既有 scheduler/MiniQMT OMS/旧 schedule-window 断言基线，不是本分支回归。
  - `paper_v2_l3` 因 Validation Center runner 未启用且资源预算不足明确 deferred；由 GitHub CI/nightly 承接，不把 deferred 报告为 PASS。
  - 请求的 DeepSeek planner 未实际调用，receipt 明确为 `provider=deterministic`、`llm_invoked=false`；不把它描述为 LLM 设计评审通过。

### DESIGN-COMPLIANCE-001

- `no_simplified_delivery=PASS`：不是只验证 create；SINGLE_ALPHA 与 MULTI_ALPHA 均覆盖完整 LocalSIM run/order/fill/cash/snapshot/events 链，MiniQMT/QMT 与 live/readiness 残留调用同步处理。
- `no_silent_error=PASS`：未新增异常捕获或 fallback；数据、policy、account/slot、artifact authority、订单和账本错误继续 fail-loud；retired binding 使用稳定 reason code。
- `no_business_semantic_drift=PASS`：未修改信号、topK、目标权重、调仓、算法、价格、成交、现金、持仓、调度和 broker side effect；只删除与用户边界冲突的重复包完整性调用。
- `no_unrequested_gate_or_approval=PASS`：未新增状态、approval、RBAC、人工 acknowledge 或 confirmation；retired lifecycle 拒绝保留既有行为而不执行 asset revalidation。

## Rollout / Rollback / 发布回滚

### Rollout

代码合入后只改变后续请求是否重复执行 StrategyPackage 完整性检查，不改变生产数据、package manifest、receipt、selection artifact 或 portfolio/run schema。新进包仍必须在 writer 路径完成一次性准入。无需 DML/DDL、无需 broker 操作；服务是否重启由用户另行决定。

### Rollback

代码回滚即可恢复旧二次校验调用；无 DB 或资产回滚。若发现缺失的入口 writer，应修复该 writer 使其执行一次性准入，不能在运行路径恢复 full self-check、model probe 或静默 fallback。

## Risks / Failure Modes / 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 某个新包 writer 未附 receipt | 未准入包可能进入下游 | writer 清单与直接测试；修 writer，不在 runtime 补二次检查 |
| 删除 manifest validator 同时误删 artifact identity | 信号串包/漂移 | artifact 五元 identity 与 authority tests 必须保留 |
| 把执行 policy/data checks 误判为包门禁删除 | 错误订单或数据源 | F-009 明确保留并跑回归 |
| multi runtime 回读 child package | 破坏父包自包含 | child-failing repository killer test |
| 历史无 receipt 包被当作新包承诺 | 兼容范围漂移 | F-010 明确不回填，历史包退役 |
| 构造器兼容 seam 被误认为仍在执行校验 | 维护误解 | 代码注释与 raising sentinel 证明不调用 |
| 删除错误后返回成功 | 假成功 | run/order/fill/ledger 业务 oracle 与失败回归 |

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：无 schema/DDL。
- `production_dml_gate=noop`：无 backfill、manifest/receipt/portfolio 数据写入。
- `production_backend_dependency_gate=noop`：无 Python dependency 变化。
- `production_frontend_dependency_gate=noop`：无 frontend dependency 变化。
- `production_config_gate=noop`：无生产配置变化。
- `broker_side_effect=none`：不调用 broker，不下单、不查账户。
- `service_restart=not_performed`：不启动、停止或重启任何服务。
- `protected_asset_mutation=none`：不修改 frozen package/model/factor/selection artifacts。
