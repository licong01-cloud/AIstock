# L3 UI Validation - qe_eval_v2 WVMA5 Recompute - 2026-05-01

## Scope

Validate that the updated official independent metric calculation path can be executed from the QuantEvolver factor library UI and that all metrics except the intentionally changed `coverage` field remain identical for the same factor and same snapshot date.

## Environment

- Backend: `http://127.0.0.1:8012`, restarted from commit `6bef67e` only; production `8001` was not touched.
- Frontend: `http://127.0.0.1:3012`, `NEXT_PUBLIC_API_BASE=http://127.0.0.1:8012/api/v1`.
- UI route: `/quantevolver/factors`.
- Snapshot selected in UI: `20260410`.
- Factor selected in UI: `WVMA5` from source `alpha158`.

## UI Operation

1. Open `/quantevolver/factors`.
2. Select data snapshot `20260410`.
3. Set source filter to `alpha158`.
4. Search `WVMA5`.
5. Select the `WVMA5` row checkbox.
6. Click `计算指标(1)`.
7. Accept the confirmation dialog.

UI result text contained `指标获取完成` and the backend response was successful.

```json
{
  "success": true,
  "requested_factors": ["WVMA5"],
  "eligible_factors": ["WVMA5"],
  "snapshot_date": "2026-04-10",
  "db_result": {"inserted": 5, "skipped": 0, "errors": [], "calc_engine": "qe_eval_v2", "recovered_from_db": true},
  "success_count": 1,
  "fail_count": 0,
  "dispatch_status": "success",
  "recovery_reason": "db_metrics_complete"
}
```

## Before / After DB Records

- Before calculation timestamp: `2026-05-01T08:30:41.115900+08:00`.
- After calculation timestamp: `2026-05-01T14:49:40.214976+08:00`.
- Rows before: 5.
- Rows after: 5.
- Compared fields excluded only runtime identifiers and the intentional algorithm-change field: `coverage`, `calculated_at`, `calc_batch_id`.
- Max absolute difference for all compared metrics: `0.0`.
- Mismatch count for all compared metrics: `0`.

## Coverage Changes

| eval_window | before | after | delta |
| --- | ---: | ---: | ---: |
| full | 0.8404271505319362 | 0.9988679416024517 | 0.15844079107051556 |
| out_sample | 0.9949827365830632 | 0.9990154137920185 | 0.004032677208955282 |
| recent_1m | 0.9981096994112179 | 0.9991304148836357 | 0.001020715472417888 |
| recent_3m | 0.9968529421891678 | 0.9985782972430397 | 0.0017253550538719464 |
| recent_6m | 0.9971645491168267 | 0.9987757726283898 | 0.0016112235115630646 |

## Conclusion

- The UI path successfully executed official independent metric calculation after the AIstock-owned `qe_eval_v2` kernel migration.
- For the same factor and same snapshot, all non-coverage metrics were byte-for-byte/numerically identical in DB comparison.
- `coverage` changed as expected because the denominator semantics changed from raw matrix density to PIT listed/tradable/non-warm-up coverage with suspension exclusion.
- This validates that the coverage change did not alter IC, RankIC, Sharpe, annual return, drawdown, monotonicity, turnover, horizon IC, direction, best horizon, or other official metrics for this sample factor.
