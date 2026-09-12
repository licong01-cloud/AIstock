# Advisory 历史范围同批次业务对比 F1 详细设计 v1.0

> 日期：2026-09-13
> 状态：`PR_4635_OPEN_CI_PENDING`
> 归属：Selection Center / Advisory
> 级别：F1（Advisory 单模块 API、服务和页面增量）

## 1. 目标

当 QE 正式交付新的 StrategyPackage 后，用户必须能在同一历史范围批次、同一冻结日期计划和同一结果口径下，把候选 Program 与基线 Program 的业务结果并排比较，而不需要下载完整 summary 证据对象或人工拼接 JSON。

本切片只投影已有不可变结果，不生成新研究证据，不训练模型，不晋升 StrategyPackage，不改变 Selection 排名、Advisory 决策或激活状态。

## 2. Background / 当前事实与问题

1. `HistoricalRangeResearchBatchRequestV1.program_specs` 已允许一个批次包含多个 Program；同一批次共享起止日期和冻结规划。
2. `HistoricalRangeQueryRepository.list_runs()` 已能列出同批次 Program run，并暴露 package、进度和最新 summary 身份。
3. `HistoricalRangeSummaryArtifactV2.metrics` 已包含绝对/超额收益、胜率、赔率、最大回撤、换手等指标；每项指标携带 coverage。
4. `HistoricalRangeResearchView` 当前只能逐个进入 Program run 查看 summary，不能直接选择基线与候选并比较。
5. 现有 `list_summaries()` 返回 summary 全行和完整 artifact JSON。完整证据适合审计，不适合页面的常规对比读取。

因此，缺口不是重做回放或重新计算指标，而是提供一个只读、可交互、显式校验可比性的业务投影。真实 DEV Summary 含约 9 万条逐日 recall 诊断；它们不是业务聚合指标，不能原样进入常规比较响应。

## 3. 允许复用的 API 与实现模式

| 能力 | 权威实现 | 本切片用法 |
|---|---|---|
| 同批次 run 身份 | `backend/services/advisory_historical_range/query_repository.py` 的 `get_run()` / `list_runs()` | 校验两个 run 存在且属于同一 batch |
| 最新 summary 身份 | `get_run()` 的 `latest_summary_id/latest_summary_version/latest_summary_ref` | 绑定本次只读投影的证据版本 |
| summary 指标合同 | `backend/services/advisory_historical_range/models.py` 的 `HistoricalRangeSummaryArtifactV2` | 只投影 `metrics/unavailable_metrics` 及其 hashes，不改变 producer 语义 |
| day 状态 | `HistoricalRangeDayStatus` | 统计 `COMPLETE/VALID_NO_CANDIDATE/WAITING/FAILED`，其中无候选使用现有 `VALID_NO_CANDIDATE`，不新增同义状态 |
| HTTP 错误映射 | `backend/routers/advisory.py` 的 `_historical_range_call()` | 保持 reason_code/context 可见 |
| 前端严格解析 | `frontend/src/lib/api/advisory.ts` 的 historical-range require helpers | 新响应做结构校验，禁止把缺字段当空结果 |
| 历史验证页面 | `HistoricalRangeResearchView.tsx` | 在已选 batch 的 run 区域增加基线/候选选择和对比表 |

禁止假设存在新的 QE API、StrategyPackage 晋升 API 或模型角色资产；本功能不调用这些写接口。

## 4. 范围

### 4.1 包含

- 新增同批次两个 range run 的紧凑只读 comparison query。
- 校验 batch/run/summary、summary policy 和 producer code 身份。
- 按 `metric_key + group_key` 对齐 AVAILABLE 与 unavailable 指标。
- 对双方均为 AVAILABLE 且数值合法的指标计算 `candidate_minus_baseline`；不输出显著性、胜负或激活建议。
- 返回两个 run 的 day 状态计数，显式展示 `VALID_NO_CANDIDATE`。
- UI 支持从同一 batch 选择不同的基线/候选 run，展示身份、可比性、关键业务指标和完整业务聚合指标表。
- 逐日 `strategy_recall@K:YYYY-MM-DD:*` / `conditional_recall@K:YYYY-MM-DD:*` 诊断不进入交互指标表，但响应必须分别报告 AVAILABLE/UNAVAILABLE 的精确省略数量；既有 Summary 明细接口继续保留原始证据。

### 4.2 Non-goals / 不包含

- 不提交 QE 实验，不读取或修改 QE 公共源码。
- 不创建、晋升、退役或修改 StrategyPackage。
- 不刷新 outcome，不重跑历史范围，不生成新的 summary/artifact。
- 不把 aggregate delta 冒充 paired inference，不计算置信区间或显著性。
- 不做模型激活、资金仓位、订单或交易执行。
- 不做 DDL/DML、依赖安装或进程控制。

## 5. API 合同

新增：

`GET /api/v1/advisory/historical-range-batches/{batch_id}/comparison?baseline_range_run_id=...&candidate_range_run_id=...`

响应核心：

```json
{
  "ok": true,
  "data": {
    "comparison": {
      "schema_version": "advisory_historical_range_comparison_v1",
      "batch_id": "ahrb_...",
      "baseline": {
      "range_run_id": "ahrr_...",
      "research_program_id": "hrp_...",
      "package_id": "pkg_...",
      "summary_id": "ahrs_...",
      "summary_version": 2,
      "summary_artifact_hash": "..."
    },
      "candidate": {},
      "comparability": {
      "status": "COMPARABLE",
      "blockers": [],
      "warnings": [],
      "summary_policy_hash": "...",
      "producer_code_hash": "...",
      "decision_use": "BUSINESS_VALIDATION_ONLY"
    },
      "day_support": {
      "baseline": {"status_counts": {"COMPLETE": 40, "VALID_NO_CANDIDATE": 4}, "successful_day_count": 44, "valid_no_candidate_day_count": 4},
      "candidate": {"status_counts": {"COMPLETE": 38, "VALID_NO_CANDIDATE": 6}, "successful_day_count": 44, "valid_no_candidate_day_count": 6}
    },
      "omitted_diagnostics": {
      "reason": "HIGH_CARDINALITY_PER_DATE_RECALL_NOT_A_BUSINESS_AGGREGATE",
      "baseline": {"available_daily_recall": 0, "unavailable_daily_recall": 93500},
      "candidate": {"available_daily_recall": 0, "unavailable_daily_recall": 94150}
    },
      "metrics": [
      {
        "metric_key": "...:mean_return",
        "group_key": null,
        "baseline": {"status": "AVAILABLE", "value": "0.01", "coverage": {}},
        "candidate": {"status": "AVAILABLE", "value": "0.02", "coverage": {}},
        "delta": "0.01",
        "delta_semantics": "CANDIDATE_MINUS_BASELINE"
      }
      ]
    }
  }
}
```

### 5.1 可比性状态

- `COMPARABLE`：两个不同 run 属于请求 batch，双方有 latest summary，且 `summary_policy_hash` 与 `producer_code_hash` 相同。
- `INCOMPLETE_EVIDENCE`：任一 run 尚无 summary；返回身份与 blocker，不伪造零指标。
- `INCOMPATIBLE`：summary policy 或 producer code 不同；返回 blocker，指标可用于诊断展示，但 `delta=null`。

非法 batch/run 身份、同一 run 自比或 run 不属于 batch 继续使用 typed HTTP 4xx。

### 5.2 指标对齐

- key 为 `(metric_key, group_key)`；两侧各保留 `status/value/coverage/reason_code`。
- 缺失、unavailable、非有限或非 Decimal 数值均令 `delta=null`。
- 只有顶层 `COMPARABLE` 时允许 delta。
- delta 只是聚合指标差值，不是 paired day 统计推断，不得输出 `better/winner/pass`。
- “完整”限定为完整业务聚合指标：不得隐藏不利、UNAVAILABLE 或双方 key 不一致的聚合项；高基数逐日 recall 诊断按冻结模式剔除并返回精确计数，不得按结果好坏选择性剔除。

## 6. 数据读取与性能

新增 query 只选择：

- run 身份与 package 信息；
- latest summary 的 id/version/artifact hash；
- `summary_json` 中的 `summary_policy_hash`、`producer_code_hash`、业务聚合 `metrics/unavailable_metrics`，以及被剔除逐日 recall 诊断的精确计数；
- day status 聚合计数。

不得经由 `list_summaries()` 把 `covered_outcome_refs`、全量 `maturity_coverage` 或高基数逐日 recall 诊断返回给比较页面。数据库连接沿用现有 read-only、repeatable-read 查询路径。DEV 只读实测（2026-09-13，同一真实 batch 两个 run，连续三次）从约 94,912 个 union 指标缩减为 762 个业务聚合指标，序列化响应约 368,114 bytes，查询耗时 3.912～4.241 秒；两侧分别显式报告省略 93,500 / 94,150 个 unavailable 逐日 recall 诊断。

## 7. Implementation Plan / 实施方案

1. 在 historical-range query repository 增加单次读取两个 run 的紧凑 comparison facts 查询；SQL 只投影必要 JSON 字段和 day status 聚合。
2. 新建纯函数 comparison builder，完成身份校验、指标对齐、Decimal delta 和 typed comparability。
3. application service 与 router 暴露只读 GET；沿用现有 historical-range 错误 envelope。
4. 前端 API 增加严格类型与响应校验；历史验证页增加同批次 baseline/candidate 选择和结果展示。
5. 更新蓝图进度、验收矩阵和源码状态后执行最终门禁。

## 8. UI 行为

1. 只有同一 batch 至少两个 run 时展示比较区。
2. 基线和候选必须不同；run 变化使旧 comparison 失效。
3. 顶部展示 package/run/summary 身份与可比性 blocker/warning。
4. 关键指标优先筛选包含 `RETURN_NET_ABSOLUTE`、`RETURN_NET_EXCESS` 且以 `mean_return/win_rate/max_drawdown/turnover` 结尾的项。
5. `VALID_NO_CANDIDATE` 单独展示为“无合格荐股日”，不折算为亏损日或零收益日。
6. 完整业务聚合指标表仍可展开，禁止只展示有利或 AVAILABLE 指标；页面同时显示两侧被省略逐日 recall 诊断总数和明细证据入口说明。

## 9. Risks / 失败模式与安全语义

- 风险：summary 缺失被误当成零结果。处理：返回 `INCOMPLETE_EVIDENCE`，不是空数组成功。
- 风险：policy/code identity 不同仍计算差值。处理：返回 `INCOMPATIBLE`，全部 delta 置空。
- 风险：aggregate delta 被误读为统计显著性。处理：固定 `CANDIDATE_MINUS_BASELINE` 和 `BUSINESS_VALIDATION_ONLY`，不输出 winner/pass。
- 风险：常规 UI 拉取完整大 artifact 或约 9 万条逐日 recall 诊断。处理：SQL 仅投影业务聚合字段，用固定 metric-key 模式排除逐日诊断并返回精确计数；测试断言 query shape，DEV 只读 smoke 验证响应规模。
- 风险：性能裁剪演变为选择性指标展示。处理：只允许剔除预定义的逐日 recall 诊断；所有候选、episode、list-version、range 业务聚合项及其 unavailable 状态均保留。
- 查询异常沿现有 historical-range typed error 返回；前端展示 reason_code/correlation_id。
- endpoint 为纯 GET，不改变 batch、run、summary、Program 或 package。

## 10. Verification Plan / 验证方案

- Service/query：同批次成功、不同 batch、同 run、自缺 summary、policy mismatch、code mismatch、unavailable/非法数值、day 状态计数。
- API：参数必填、typed 4xx、响应 envelope。
- UI：同 batch 选择、禁止自比、COMPARABLE 表格、INCOMPLETE/INCOMPATIBLE 提示、无合格荐股日、省略诊断计数、请求失败可见。
- 回归：historical-range backend 小矩阵、Advisory page Playwright、TypeScript、Ruff、`git diff --check`、模块边界扫描。

## 11. Design Acceptance Index

| ID | 条款 |
|---|---|
| F-001 | 只允许同一 batch 的两个不同 range run 进入比较 |
| F-002 | summary 缺失、policy/code 不同必须 typed 可见且不得计算 delta |
| F-003 | 业务聚合指标按 `metric_key + group_key` 完整对齐，不隐藏 unavailable；仅按冻结模式排除逐日 recall 诊断并报告精确计数 |
| F-004 | delta 仅为 COMPARABLE 下的 candidate-minus-baseline 聚合差值，不冒充统计显著性 |
| F-005 | day support 显式区分 `VALID_NO_CANDIDATE`，不折算为亏损或零收益 |
| F-006 | query 返回可交互 summary 投影，不读取接口级完整大 artifact 或高基数逐日 recall 诊断 |
| F-007 | UI 显示身份、阻断、关键指标、完整业务聚合指标和省略诊断计数，错误不静默 |
| F-008 | 仅改 Advisory 源码/测试/设计；QE、Selection、StrategyPackage、DB、进程均不变 |

## 12. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_historical_range/comparison.py`; `query_repository.py`; `service.py` | `backend/tests/advisory_historical_range/test_comparison.py` | implemented_verified | none |
| F-002 | `backend/services/advisory_historical_range/comparison.py` | `backend/tests/advisory_historical_range/test_comparison.py` | implemented_verified | none |
| F-003 | `backend/services/advisory_historical_range/comparison.py` | `backend/tests/advisory_historical_range/test_comparison.py` | implemented_verified | none |
| F-004 | `backend/services/advisory_historical_range/comparison.py` | `backend/tests/advisory_historical_range/test_comparison.py` | implemented_verified | none |
| F-005 | `backend/services/advisory_historical_range/query_repository.py`; `HistoricalRangeComparisonPanel.tsx` | `backend/tests/advisory_historical_range/test_comparison.py`; `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | implemented_verified | none |
| F-006 | `backend/services/advisory_historical_range/query_repository.py` | `backend/tests/advisory_historical_range/test_comparison.py` | implemented_verified | none |
| F-007 | `HistoricalRangeComparisonPanel.tsx`; `useHistoricalRangeResearch.ts`; `frontend/src/lib/api/advisory.ts` | `frontend/tests/paper-v2/paper-v2-advisory-historical-range.spec.ts` | implemented_verified | none |
| F-008 | changed-file scope scan | `backend/tests/advisory_historical_range/test_comparison_api.py`; `scripts/aistock_feature_workflow.py` | implemented_verified | none |

## 13. Production Gates / 生产门禁

- `production_ddl_gate=noop`：不改 schema，不执行 DDL/DML。
- `dependency_gate=noop`：不增加依赖。
- `backend_restart_required=true`：新增 router 端点合入后需要用户自行重启才能运行时加载；源码合入不代表运行时激活。
- `qe_experiment_gate=noop`：本功能不提交 QE 或 Advisory 模型实验。
- `strategy_package_mutation=noop`：不创建、晋升、退役或修改包。

## 14. DESIGN-COMPLIANCE-001 预检查

1. 禁止简化交付：backend、API、UI 和测试必须同时完成后才可声明交付。
2. 禁止静默错误：缺 summary、不可比和 unavailable 指标均 typed 展示。
3. 禁止改变业务逻辑：只读投影现有 summary，不重新定义收益、胜率或决策。
4. 禁止私增门禁审批：comparison 不阻断 Program/binding/replay；激活仍使用既有合同。

## 15. 终止条件

- F-001～F-008 全部有实现与测试证据；
- F1 validator、定点测试、前端测试与边界扫描通过；
- 多轮审核无未解决的 P0/P1 问题；
- PR CI 通过并完成合入和安全清理；
- 若运行时需要加载新 endpoint，则状态停在 `runtime_activation_pending_user_restart`，不由本任务控制进程。
