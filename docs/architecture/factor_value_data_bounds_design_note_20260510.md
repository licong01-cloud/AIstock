# factor_value ingestion bounds — design note (P2.b follow-up)

> **Author**: Claude Code dw-foundation-lead
> **Date**: 2026-05-10
> **Status**: DESIGN NOTE — open question pre-worker-enable
> **Triggered by**: Codex T14b/T14c review round 2, P2.b

## §1 Scope

`factor.recompute.completed` payloads carry `data_start` and `data_end` as
identifiers indicating the time range the recompute touched. The
`FactorValueArchiveHandler` currently treats them as **informational only** — it
asks `source_loader(payload)` for whatever rows the source produces, then
upserts **every row returned** into `qe_archive.factor_value`.

The default source loader reads `rdagent_assets/factor_values/single/{factor_name}.parquet`
in its entirety. There is no slice on `data_start..data_end`.

This is acceptable in dev/test where:
- the test source loader returns small in-memory lists;
- the parquet path is single-factor scoped;
- the qe_archive worker is disabled by default.

It is **NOT** acceptable when the worker is enabled in production:
- a single recompute that touches 8 years × 5000 stocks ≈ 50B factor-value rows
  would dominate the archive write budget;
- a recompute that legitimately re-derives only the last 30 days would still
  trigger a full historical re-upsert on archive (idempotent, but wasteful);
- monitoring becomes impossible because every event reports the same row count
  regardless of what changed.

## §2 The open question

Which of these does the production ingestion contract pick?

### Option A — handler narrows by data_start/data_end

```python
def _default_source_loader(payload):
    df = pd.read_parquet(parquet_path)
    if "data_start" in payload and "data_end" in payload:
        df = df[(df["trade_date"] >= payload["data_start"]) &
                (df["trade_date"] <= payload["data_end"])]
    return df.to_dict(orient="records")
```

Pros: bandwidth bounded by the recompute window declared in the payload.
Cons: requires the producer (factor pipeline) to accurately report its
recompute window. If the pipeline lies (e.g. always says full range), Option A
becomes Option B in practice.

### Option B — accept the producer's full data_start..data_end as gospel

Same as today, but add an explicit assertion in the handler that the rows
returned span exactly `data_start..data_end`. Reject otherwise.

### Option C — payload carries a list of (factor_name, code, trade_date) keys

Most explicit; producer enumerates exactly which rows changed. Works well for
incremental factor updates; doesn't work for first-time factor onboarding.

## §3 Recommendation

**Defer until worker enable.** When ops authorize the qe_archive worker for
production:

1. Pick Option A (narrowest, simplest).
2. Add a test that asserts the handler ignores rows outside
   `[data_start, data_end]`.
3. Add a producer-side smoke that the recompute pipeline emits
   `data_start = min(touched_trade_dates)` and `data_end = max(touched_trade_dates)`.

## §4 What's IN scope for round-2 fix

- **Out of scope**: source loader narrowing logic (deferred per §3).
- **In scope**: the missing-key fail-fast (P2.a) — handler raises if any row
  lacks `trade_date` or `code`. This catches loader bugs immediately rather
  than silently dropping rows.

## §5 Tracking

This design note registers the gap. When the worker enable PR is opened, link
back here. No code change required from this note alone.
