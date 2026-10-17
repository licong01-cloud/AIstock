# STAGE1 验收文档

分支/commit: codex/price-guard-stage1-20260602 / 实现 commit: 2141163e；验收文档 commit: 本文件所在 HEAD

本文第1—5节保留 Stage1 原交付时点的验收范围，不代表当前系统或运行态的整体状态。2026-10-02 的证据订正及 follow-up #585 状态见第6节。

## 1. 功能清单完成度 (M1-M4)

| 里程碑 | 状态 | 关键文件:行 | 测试名 |
|---|---|---|---|
| M1 契约与核心 evaluator | DONE | backend/services/trading_core/price_guard.py:112, backend/services/trading_core/price_guard.py:155, backend/services/trading_core/price_guard.py:205, backend/services/trading_core/exit_guard.py:51, backend/services/trading_core/exit_guard.py:95, backend/services/trading_core/exit_guard.py:136, backend/services/strategy_package/execution_policy.py:25 | backend/tests/trading_core/test_price_guard.py::test_s1_1_price_guard_is_pure_and_deterministic; backend/tests/trading_core/test_exit_guard.py::test_s1_1_exit_guard_is_pure_and_deterministic; backend/tests/strategy_package/test_execution_policy_price_guard.py::test_s1_3_policy_hash_stable_and_disabled_key_is_distinct |
| M2 选股买入区间 + 止损展示 | DONE | backend/services/selection_center/price_guidance.py:74, backend/services/selection_center/price_guidance.py:83, backend/services/selection_center/models.py:44, backend/services/selection_center/repository.py:120, backend/db/init_trading_core_v2_schema.py:312, frontend/src/app/paper-v2/selection/page.tsx:421 | backend/tests/selection_center/test_price_guidance.py::test_s1_4_s1_5_selection_guidance_fields_tick_limits_disclaimer_and_status |
| M3 自选池 + 荐股生命周期 + 每日复评 | DONE | backend/db/init_watchlist_schema.py:75, backend/services/advisory_lifecycle.py:206, backend/services/advisory_lifecycle.py:229, backend/repositories/watchlist_repo_impl.py:307, backend/services/watchlist_service.py:527 | backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_6_lifecycle_state_machine_legal_and_illegal_transitions; backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_7_daily_review_is_append_only_idempotent_and_updates_holding_once |
| M4 区间/止损质量回顾评估 | DONE | backend/services/advisory_quality.py:49 | backend/tests/watchlist/test_advisory_quality_report.py::test_s1_11_quality_report_metrics_buckets_and_parent_shrink |

## 2. 验收标准逐条

| # | 标准 | PASS? | 证据(测试名/命令/产物路径) |
|---|---|---|---|
| S1-1 | evaluator 纯函数、同输入同输出 | PASS | backend/tests/trading_core/test_price_guard.py::test_s1_1_price_guard_is_pure_and_deterministic; backend/tests/trading_core/test_exit_guard.py::test_s1_1_exit_guard_is_pure_and_deterministic |
| S1-2 | green/yellow/red、SKIP、REDUCE、near_limit、breakout、缺 signal_ref_price/basis mismatch fail-fast | PASS | backend/tests/trading_core/test_price_guard.py::test_s1_2_green_yellow_red_and_chase_decisions; backend/tests/trading_core/test_price_guard.py::test_s1_2_near_limit_and_breakout_addon; backend/tests/trading_core/test_price_guard.py::test_s1_2_fail_fast_missing_signal_ref_price_basis_and_limits |
| S1-3 | policy hash 稳定、缺省 vs enabled:false 不同 hash 同语义、validator 拒未知/拒 algo_config 夹带 | PASS | backend/tests/strategy_package/test_execution_policy_price_guard.py::test_s1_3_policy_hash_stable_and_disabled_key_is_distinct; backend/tests/strategy_package/test_execution_policy_price_guard.py::test_s1_3_validator_allows_guard_contract_keys_and_rejects_unknown; backend/tests/strategy_package/test_execution_policy_price_guard.py::test_s1_3_validator_rejects_algo_config_guard_parameter_smuggling |
| S1-4 | 选股区间字段齐全；缺 signal_ref_price/basis/hash 不填默认价 | PASS | backend/tests/selection_center/test_price_guidance.py::test_s1_4_s1_5_selection_guidance_fields_tick_limits_disclaimer_and_status; backend/tests/selection_center/test_price_guidance.py::test_s1_4_missing_signal_ref_price_degrades_without_default_price; backend/tests/selection_center/test_price_guidance.py::test_s1_4_basis_mismatch_fails_fast |
| S1-5 | tick 取整、涨跌停边界、免责标注、guidance_status=rule_default | PASS | backend/tests/selection_center/test_price_guidance.py::test_s1_4_s1_5_selection_guidance_fields_tick_limits_disclaimer_and_status; backend/tests/selection_center/test_price_guidance.py::test_s1_5_attach_guidance_keeps_all_generated_rows_rule_default; frontend/src/app/paper-v2/selection/page.tsx:421 |
| S1-6 | 生命周期状态机合法/非法迁移 | PASS | backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_6_lifecycle_state_machine_legal_and_illegal_transitions |
| S1-7 | 每 item×交易日 append-only，一日一行，幂等 | PASS | backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_7_daily_review_is_append_only_idempotent_and_updates_holding_once; backend/tests/watchlist/test_advisory_schema_contract.py::test_s1_7_advisory_schema_contract_is_append_only_and_commented |
| S1-8 | 当日建仓硬止损 STOP_LOSS_DEFERRED_T1，不立即 EXITED | PASS | backend/tests/trading_core/test_exit_guard.py::test_s1_8_hard_stop_deferred_t1_and_later_exits; backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_8_same_day_hard_stop_deferred_t1_does_not_exit |
| S1-9 | 复权调整 stop/take；停牌 WAITING/carry | PASS | backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_9_next_day_stop_rank_drop_factor_adjustment_and_suspend_carry; backend/tests/trading_core/test_exit_guard.py::test_s1_9_exit_guard_suspend_waiting_and_fail_fast_inputs |
| S1-10 | advisory 路径零 OMS/broker/Paper ledger 写入 | PASS | backend/tests/watchlist/test_advisory_lifecycle.py::test_s1_10_advisory_lifecycle_has_no_oms_broker_or_paper_ledger_writes; `rg "create_order|submit_order|position_ledger|paper_v2\." backend/services/advisory_lifecycle.py backend/services/advisory_quality.py` 无命中 |
| S1-11 | 质量报告 + 分桶 + post-decision diagnostics + 无未来函数 | PASS | backend/tests/watchlist/test_advisory_quality_report.py::test_s1_11_quality_report_metrics_buckets_and_parent_shrink; backend/tests/watchlist/test_advisory_quality_report.py::test_s1_11_quality_report_rejects_future_fields_in_decision_inputs; backend/tests/watchlist/test_advisory_quality_report.py::test_s1_11_quality_report_is_reproducible |
| S1-12 | 无 silent fallback、缺输入 fail-fast、现状回归不变 | 范围内扫描已复核；历史软提示仍待确认 | 原 `--changed-only` 扫描0文件，其 findings=0 不能作为验收证据；原175项测试为历史记录，不冒充本次运行。2026-10-02 显式扫描原6个核心文件：files=6、findings=0、blocking=0；命令、源码修复和剩余限制见第6节。 |

## 3. 偏离设计之处 (如有) + 理由

- 无功能裁剪；Stage 1 只实现 advisory/selection/watchlist/diagnostics，不实现 Stage 2 QE/Paper v2 enforced。
- `selection.package_result.reference_price` 现有语义保持只读不改：当前由 Selection Center enrichment 写入入池/展示参考价，常来自 PIT close 或 TDX current/pre_close，并继续用于 watchlist import 的 entry_price；本次新增 `signal_ref_price` 字段与 guidance JSON，不复用 `reference_price` 表达 PriceGuard 信号价。
- `app.advisory_daily_review.evidence_id` 在 migration 中直接 FK 到 `selection.daily_selection_evidence`；`init_watchlist_schema.py` 为兼容单独初始化场景，采用表存在时再补 FK 的幂等写法。
- 前端 lint 未完成：`npm run lint -- --file src/app/paper-v2/selection/page.tsx --file src/lib/paper-v2/types.ts` 输出 `next is not recognized`，原因是实现 worktree 的 `frontend/node_modules` 不存在；未安装依赖，未启动服务。

## 4. 如何复核 (审核者可直接跑)

- `python -m compileall backend/services/trading_core/price_guard.py backend/services/trading_core/exit_guard.py backend/services/selection_center/price_guidance.py backend/services/advisory_lifecycle.py backend/services/advisory_quality.py backend/services/strategy_package/execution_policy.py backend/services/selection_center/models.py backend/services/selection_center/service.py backend/services/selection_center/repository.py backend/db/init_trading_core_v2_schema.py backend/db/init_watchlist_schema.py`
- `pytest backend/tests/trading_core backend/tests/selection_center/test_price_guidance.py backend/tests/selection_center/test_result_enrichment.py backend/tests/selection_center/test_selection_center_api.py backend/tests/selection_center/test_runtime_selection.py backend/tests/strategy_package/test_execution_policy_price_guard.py backend/tests/strategy_package/test_validation_stability.py backend/tests/watchlist/test_advisory_lifecycle.py backend/tests/watchlist/test_advisory_quality_report.py backend/tests/watchlist/test_advisory_schema_contract.py -q -p no:cacheprovider`
- `python scripts/aistock_guardrail_scan.py backend/services/trading_core/price_guard.py backend/services/trading_core/exit_guard.py backend/services/selection_center/price_guidance.py backend/services/advisory_lifecycle.py backend/services/advisory_quality.py backend/services/strategy_package/execution_policy.py --fail-on-severity P1`
- `git diff --check`

## 5. 已知限制 / 下阶段衔接

- Stage 2 QE 注入、A/B enforced、Paper v2 parity/live limit fill 仍未实现，等待审核 PASS 后再进入。
- `bucket_calibrated` / `ml_residual_alpha_v1` / `ml_exit_v1` 仅保留 mode 与分支入口，Stage 1 按计划 fail-fast/NotImplemented，不宣称 ML 或 QE 验证。
- 所有 Stage 1 guidance_status 均为 `rule_default`；没有任何 `qe_validated` 标识。
- DDL 只产出迁移脚本与 init schema 更新，未应用生产库；production_ddl_gate=pending。
- 未启动 backend/frontend/TDX 服务；production_frontend_dependency_gate=noop，production_backend_dependency_gate=noop。

## 6. 2026-10-02 证据订正与 follow-up #585

本次以 `origin/main` 的 `f031502f9` 为源码检查基准，只做只读扫描、现有定向测试和运行身份读回，没有数据库写入、服务控制或研究重跑。

| follow-up条目 | 证据与结论 | 状态 |
|---|---|---|
| #585-1：S1-12 空扫描 | 第4节显式命令实际扫描6个文件，files=6、findings=0、blocking=0；不将规则扫描零问题等同于所有业务语义已验收 | 本文已订正 |
| #585-2：watchlist 两处旧 silent fallback | BUG-1123 / PR #3587（merge `c0a89782e50faf942066dacc195844b82d78a104`）已为 TDX 失败和 xtquant 行解析失败增加聚合 warning；同时非法 PIT 日期 fail-fast、历史查询失败记录 warning。范围限于原两处 quote lookup，不宣称其他历史异常处理全部清零 | 源码已合入；定向测试通过 |
| #585-3：两个软提示的意图 | 下述现存兼容行为已核对，但本次未改变或新增授权；不能因扫描通过就认定缺输入时严格价格/复权合同通过 | 仍待业务owner确认，不关闭整个 #585 |

BUG-1123 定向命令：

`rtk pytest -q -p no:cacheprovider --basetemp=X:/AIstock_temp/advisory/legacy-1123-20261002 backend/tests/selection_center/test_result_enrichment.py backend/tests/watchlist/test_realtime_amount_units.py`

本次结果为 **23 passed**，覆盖非法 PIT 日期拒绝、历史查询失败审计、行情失败日志及已有日频数据库读取约束。运行态官方 `post-restart-verify` 对源 merge 身份的只读复验通过；其中 OpenAPI smoke 不是这些业务修复的单独证明，业务修复仍以该源提交和上述定向测试为依据。

两项软提示的现状和限制：

- `selection_center/price_guidance.py` 缺 previous_close 时使用 signal_ref_price 估算涨跌停边界，并输出 `limit_source=estimated_from_signal_ref_price_or_previous_close`、`guidance_status=rule_default`。这是历史规则提示，不是原生 previous_close 或模型预测，不可作为正式执行价格边界已验证的证据。
- `advisory_lifecycle.adjust_price_for_factor` 任一复权因子缺失或非正时返回未调整原价。这不证明跨除权日价格可比，也不证明 stop/take 的复权有效性。是否保留此兼容行为、还是在相应业务路径拒绝缺失因子，仍需业务owner确认；本次不以文档订正替代源码修复或授权。

因此，BUG-1123 的源码合入/测试/运行身份/源码树清理，与 #585 整体闭环分别报告。#585 第3项尚未确认时，不合入会将整个关联 Issue 自动关闭的 close-sync，也不将第1—5节的历史状态升级为当前全部完成。
