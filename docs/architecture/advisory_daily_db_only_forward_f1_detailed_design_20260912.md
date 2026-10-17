# Advisory 日频数据库行情合同 F1 详细设计

> 版本：v1.0
>
> 日期：2026-09-12
>
> 状态：IMPLEMENTED_VERIFIED_RUNTIME_RESTART_PENDING
>
> 归属：Selection Center / Advisory
>
> 上位蓝图：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.54

## 1. 背景与目标

Advisory 正式日频 forward 在交易日 D 收盘后生成下一交易日 T 的推荐，并已通过 `selection_as_of_trade_date=D` 与 `target_trade_date=T` 固定无未来数据边界。评分、特征和结算主体均使用数据库记录，但 Selection 结果补充层此前只按 `trade_date >= today` 判断当前路径，导致目标日为今天或未来时仍调用 `TDX_REALTIME` 并把实时价写成入场参考价。这与“日频荐股只使用数据库日频记录，暂不考虑盘中实时荐股”的产品要求冲突。

本切片建立显式 `selection_price_mode=DAILY_DB_ONLY` 合同：Advisory 只要提供早于目标日的决策截止，就强制启用该模式；Selection 对每只候选只读 `market.kline_daily_raw` 中不晚于决策截止日的最后一个权威收盘价，不调用实时报价。若 D 日因停牌没有日线，保留候选并使用更早的最后收盘价、记录真实价格日期且将 D 日成交量记为零；数据库查询失败或截止日前从无有效收盘价时 typed fail closed，不以实时价或候选旧价格补位。

## 2. 范围

1. Advisory forward/date-context 在 `selection_as_of_trade_date < target_trade_date` 时强制写入 `selection_price_mode=DAILY_DB_ONLY`。
2. Selection 结果补充层校验 `AUTO | DAILY_DB_ONLY` 两种模式。
3. `DAILY_DB_ONLY` 只读不晚于 `point_in_time_context.reference_price_trade_date` 的逐股最后收盘价；D 日有记录时使用其成交量，正常停牌缺行时成交量为零且价格日期保留实际上一交易日。
4. DB-only 模式完全不调用 quote fetcher；缺少权威收盘价时返回 `DataUnavailableError`。
5. receipt/候选展示保存数据库价格来源与日期；盘中 `current_price` 保持空值。
6. 既有普通当日 Selection 的 `AUTO` 实时报价行为和历史 Selection 的数据库行为保持兼容。

## 3. 非目标

- 不修改 Selection score、rank、Alpha、StrategyPackage、模型或候选股票池。
- 不实现盘中实时荐股，不读取或写入 `market.quote_snapshot`。
- 不修改 QE、QE profile、数据集、Paper、QMT、订单或资金仓位。
- 不新增 DDL/DML，不启动训练/回测/实验，不控制或重启进程。
- 不在数据库缺失时回退 TDX、候选 reference price、零值或未来行情。

## 4. 时钟与数据合同

日频 forward 的合法时钟固定为：

`decision/selection/reference/universe as-of = D < target recommendation date = T`

- 模型与 Selection 只能读取 D 及之前的信息。
- 入场参考价为每只股票 `market.kline_daily_raw.close_li@max(trade_date <= D)`；正常停牌不删股票、不阻断整批，且不得向后读取 T 日价格。
- T 只用于推荐目标、后续 target-open 结算和 episode 生效日期。
- T 日 `open_li/close_li/suspend_d` 未到位时，结算保持 `WAITING_DATA`；不得回写或改变 D 日已经发布的候选与参考价。
- `DAILY_DB_ONLY` 若缺少 `reference_price_trade_date`，或该日期不早于 T，视为调用合同错误。

## 5. 接口与失败语义

运行配置新增：

```json
{
  "selection_price_mode": "DAILY_DB_ONLY",
  "advisory_date_context": {
    "selection_as_of_trade_date": "2026-09-11",
    "target_trade_date": "2026-09-14"
  }
}
```

Selection 在完成 PIT context 后读取：

- `point_in_time_context.reference_price_trade_date`
- `market.kline_daily_raw.close_li@max(trade_date <= D)`
- `market.kline_daily_raw.volume_hand`

成功结果：

- `selection_entry_price_source=market.kline_daily_raw.close:<actual_price_trade_date>`
- `selection_entry_price_time=<actual_price_trade_date>`
- `reference_price=<DB close>`
- `previous_close=<DB close>`
- `current_price/current_price_source/current_price_time=null`

失败分类：

- 未知 mode：`RuntimeConfigInvalidError`。
- D 缺失或 `D >= T`：`RuntimeConfigInvalidError`。
- DB 查询失败或候选在 D 及之前从无有效 close：`DataUnavailableError`，context 记录日期、缺失数量、样例和权威来源；单日停牌缺行不属于该失败。

## 6. 兼容性与安全

- `selection_price_mode` 缺失时按 `AUTO`，不改变其它 Selection/API 调用。
- 历史日期的 `AUTO` 路径继续优先数据库并保留既有 fallback 语义；该 fallback 不适用于 `DAILY_DB_ONLY`。
- Advisory 每次构造正式 D/T context 时覆盖为 `DAILY_DB_ONLY`，Binding 不能把它降级为 `AUTO`。
- 模式进入实际 Selection runtime config 和持久化结果，因此可审核具体运行是否使用数据库合同。
- 不引入数据库写入、schema 迁移、实时报价订阅或跨模块实验。

## 7. 实施方案

1. 在 Selection 结果补充层增加显式 `AUTO | DAILY_DB_ONLY` 价格模式；模式解析和 D/T 校验发生在任何行情读取之前。
2. `DAILY_DB_ONLY` 跳过 quote fetcher，按每只候选 `max(trade_date <= reference_price_trade_date)` 批量读取数据库日线并写入真实价格来源、日期和成交量。
3. 查询异常和候选缺少有效收盘价分别返回 typed `DataUnavailableError`，均禁止使用实时价或候选旧价补位。
4. Advisory 构造正式 D/T context 时最后写入 DB-only 模式，避免 Program binding 将正式日频调用降级成 `AUTO`。
5. 保留未显式设置模式的 Selection 兼容行为，并用现有回归覆盖历史/当日路径。

## 8. 验证方案

1. 目标日为今天或未来、D<T 时只查询 D 日数据库，quote fetcher 若被调用则测试失败。
2. DB close/volume 正确映射到候选和 display component；D 日正常停牌时使用此前最后收盘价、成交量为零且不删候选。
3. 截止日前完全无 close、查询失败、未知 mode、D>=T 均 fail closed。
4. Advisory `prepare_forward_selection` 与 `run_review_from_selection` 强制传递 `DAILY_DB_ONLY`。
5. 既有 current-day `AUTO` TDX 行为和 historical `AUTO` DB 行为回归不变。
6. Selection 定向测试、Watchlist/Advisory 回归、Ruff、diff check 和 F1 validator 通过。

## 9. 发布与回滚

源码以独立小 PR 交付。合入后由用户重启 FastAPI 后端，再以真实 Program 执行只读/preview 业务验证，核对 Selection run runtime config 和 candidate price source。无 DDL、依赖或生产数据写入。

回滚该 PR 即恢复旧 `AUTO` 行为；历史数据和已持久化结果不删除、不改写。

## Production Gates / 生产门禁

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `dependency_install=noop`
- `dataset_write_count=0`
- `qe_experiment_submission_count=0`
- `backend_restart_owner=user`

## Risks / Failure Modes / 风险

- 若单只股票 D 日正常停牌，使用 D 以前最后权威收盘价并保留实际日期；若整个截止历史从无价格或查询失败，正式荐股明确失败且不调用 TDX。
- 若调用方误把 D 设为 T 或未来日，运行配置在行情查询前失败，避免未来数据泄露。
- `AUTO` 仍保留给非 Advisory 的兼容调用；只有具备显式 D<T context 的 Advisory 正式日频路径被强制 DB-only。
- 源码合入不代表运行时已生效；用户重启和真实 Program readback分别验收。

## 10. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-001 | Advisory D<T forward 强制设置 `selection_price_mode=DAILY_DB_ONLY`，Binding 不可降级 |
| F-002 | DB-only 只读逐股不晚于D的最后数据库close，D日有行才取其volume；正常停牌保留候选并将volume记零，零实时报价调用，无历史价格或查询失败才fail closed |
| F-003 | 候选与展示保存数据库来源、实际价格交易日并清空盘中 current price |
| F-004 | 未配置模式的 Selection current-day/historical 既有行为保持兼容 |
| F-005 | 不改 QE/Alpha/StrategyPackage/股票池/交易执行；无实验、DDL/DML和进程控制 |

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_program.py::_with_advisory_date_context` | `backend/tests/watchlist/test_advisory_program.py::test_review_from_selection_accepts_target_date_with_explicit_data_cutoff` | IMPLEMENTED_VERIFIED | none |
| F-002 | `backend/services/selection_center/result_enrichment.py::SelectionResultEnrichmentService` | `backend/tests/selection_center/test_result_enrichment.py::test_daily_db_only_future_target_uses_cutoff_close_without_realtime_quote`、`::test_daily_db_only_uses_last_available_close_for_normal_suspension_gap`、`::test_daily_db_only_missing_cutoff_close_fails_closed_without_candidate_fallback`、`::test_daily_db_only_database_failure_fails_closed_without_candidate_fallback` | IMPLEMENTED_VERIFIED | none |
| F-003 | `selection_result_display` 与 `SelectionCandidate` price fields | `backend/tests/selection_center/test_result_enrichment.py::test_daily_db_only_future_target_uses_cutoff_close_without_realtime_quote` | IMPLEMENTED_VERIFIED | none |
| F-004 | `selection_price_mode=AUTO` 默认路径 | `python -m pytest backend/tests/selection_center/test_result_enrichment.py -q -p no:cacheprovider` | IMPLEMENTED_VERIFIED | none |
| F-005 | changed-file/scope scan | `python -m ruff check backend/services/selection_center/result_enrichment.py backend/services/advisory_program.py backend/tests/selection_center/test_result_enrichment.py backend/tests/watchlist/test_advisory_program.py`；`python -m nox -s watchlist_backend advisory_modeling_backend`；`git diff --check` | IMPLEMENTED_VERIFIED | none |

## 12. DESIGN-COMPLIANCE-001

- [x] 设计、实现和测试逐项映射，无 POC、placeholder 或静默 fallback。
- [x] D/T 时钟、数据库来源、价格单位和失败语义完整一致。
- [x] 真实 Selection/Advisory 调用链已接通；单元替身只验证零实时调用与错误边界，不冒充合入后业务 readback。
- [x] 兼容默认 Selection，且不改变 Alpha/rank/股票池/交易执行。
- [x] 本地门禁通过；CI、合入、用户重启和运行时验证继续作为独立状态报告。
