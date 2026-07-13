# AIstock Advisory Phase 1C-1 Capture Foundation F2 Design

## Background

This document scopes the first implementable Phase 1C slice of the approved
Phase 1 PIT observation, label, and sealed-snapshot design. Phase 1A provides
immutable source availability and exact revision-set primitives. Phase 1B
provides bounded trace envelopes and an append-only outbox, but deliberately
rejects every new outbox write until Phase 1C supplies persisted capture-batch
admission and durable gap handling.

The slice is historical research infrastructure only. It is not a model,
recommendation ranking change, portfolio operation, scheduler, or execution
feature.

## Scope

- Add additive Phase 1C schema for capture batches, immutable capture gaps,
  canonical signal headers, observation versions, observation lineage, and
  five-stage evidence/candidate rows.
- Implement the persisted capture-batch state machine with request identity,
  optimistic version, bounded lease, and monotonically increasing fencing
  token.
- Implement the transaction-bound trace admission validator consumed by the
  existing trace outbox repository.
- Implement a durable trace-gap repository and reconciler handler that keeps
  `TRACE_CAPTURE_LOST` distinct from `TRACE_WRITE_FAILED`.
- Implement fixture-only conversion of a durable trace envelope into immutable
  canonical observation, version, lineage, stage, and candidate rows.
- Keep trace capture disabled by default. A successful selection never waits
  for, depends on, or is rolled back by Phase 1C persistence.

## Non-Goals

- No source-availability observer, market-table scan, historical backfill, or
  current/latest source fallback.
- No outcome label calculation, universe denominator, build attempt,
  Parquet/CAS publish, SEALED snapshot, or training capability.
- No Advisory page/API change, simulation/Paper/QMT/MiniQMT change, broker
  integration, scheduler, or real-time data path.
- No approval, role, authorization, manual gate, or runtime DDL mechanism.
- No production DDL or production DML in this delivery.

## Architecture

```text
Historical Advisory/Selection success (existing, unchanged)
  -> bounded trace envelope (Phase 1B, explicit opt-in only)
  -> append-only trace outbox
       -> transaction-bound Phase 1C admission validation
       -> durable trace gap on an actual missing/failed capture fact
  -> fixture-only observation capture writer
       -> canonical signal / immutable version / lineage / stage records
```

The Phase 1C writer is an Advisory-owned sidecar. It consumes immutable
outbox payloads and Phase 1 control rows only. It has no import or call path to
Paper, simulation, QMT, broker, order, market-data provider, or selection
execution services.

## Contracts

### Capture batch

`advisory_capture_batch` has immutable request identity and a mutable control
header only for the documented state transition protocol:

```text
PLANNED -> RUNNING -> COMPLETE | FAILED | EXPIRED | ABORTED
```

The active worker must present both `expected_row_version` and the current
`fencing_token`. A new RUNNING lease invalidates old tokens. COMPLETE freezes a
sorted membership count/hash and a receipt hash. Terminal batches cannot accept
new membership or trace outbox writes. Exact idempotent requests return the
same batch; the same business key with a different request hash fails loudly.

The request also freezes one or more `CapturePlan` records. Each plan carries
the canonical signal header fields, Phase 0A audit/handoff/admission lineage,
and the exact source-revision-set identity required by the parent design. The
plan is immutable request content, not a lookup instruction. Before writing an
observation, the writer compares the plan's package, manifest, decision date,
handoff, and admission scope to the copied trace envelope. A missing plan field
or mismatch creates a durable capture failure/gap; it never consults a current
Program, binding, artifact, or latest selection row to fill the value.

`target_trade_date` is accepted only when an explicit frozen calendar verifier
with the same `calendar_version/hash` proves it is the immediate next trading
day after `decision_as_of_trade_date`. PostgreSQL repeats this invariant against
`market.trading_calendar`; a non-adjacent date records a capture gap and cannot
create a canonical signal.

Recovery requires the terminal predecessor id, exact predecessor row version,
and predecessor fencing token. A predecessor has at most one successor in both
the repository oracle and PostgreSQL, so an older attempt cannot be used to
fork a second attempt with the same revision number.

### Trace admission and gaps

For a new trace outbox row, the validator runs in the same transaction as the
outbox INSERT and holds the control-binding and capture-batch rows through
commit. It verifies all of the following exact identities:

- enabled persisted `TRACE_CAPTURE` control-binding event;
- event configuration equals the immutable `TraceCaptureBinding` payload;
- handoff readiness and admission scope hashes match the envelope binding;
- capture batch is RUNNING, has an unexpired lease, and exposes the supplied
  current fencing token.

An exact outbox retry remains idempotent even after the capture batch is
terminal. A new outbox row with stale, expired, disabled, missing, or divergent
state fails with an `ADVISORY_PHASE1_*` reason code; it is never silently
accepted.

`TRACE_CAPTURE_LOST` is a reconciled absence of a durable outbox row for a
frozen business-success identity. `TRACE_WRITE_FAILED` is a failed delivery of
an existing durable outbox row. They persist as different immutable gap/event
facts and neither reruns Selection nor fabricates a trace payload.

### Observation persistence

The fixture writer accepts only a durable trace envelope, not a mutable latest
selection artifact. It builds the canonical signal header, immutable
observation revision, lineage, stage summaries, and candidates from the
envelope's copied payload. It cannot infer a missing stage, component weight,
or candidate score. Missing or malformed immutable content produces a durable
PARTIAL/CAPTURE_FAILED result or a gap with a stable reason code.

No writer may overwrite an observation version, stage row, candidate, lineage,
outbox row, gap, or membership row. Revisions append a new version only under
the predecessor and content-hash constraints from the parent Phase 1 design.
The final observation content hash is computed after revision number and exact
predecessor are present in the immutable payload. PostgreSQL verifies that the
predecessor belongs to the same canonical signal and is exactly revision `n-1`.

Every native multi-Alpha INCLUDED or EXCLUDED candidate preserves an explicit
component result. FULL requires the versioned schema, payload, and payload hash;
PARTIAL/UNAVAILABLE requires stable reason codes. Missing exclusion provenance
downgrades the stage and observation rather than leaving a false FULL result.
Any calendar, plan, trace, or component validation failure is appended to the
durable gap repository before the typed error is returned.

## Design Acceptance Index

- F-001: Capture batch requests, lease/fencing, terminal transitions,
  membership sealing, and exact retry/conflict behavior are persisted and
  fail-closed.
- F-002: The real trace admission validator proves binding, readiness,
  scope, batch state, lease, and fencing identity inside the outbox write
  transaction.
- F-003: Trace loss and trace write failure become distinct durable facts;
  neither changes Selection, Advisory review, simulation, Paper, or execution
  behavior.
- F-004: Durable trace envelopes can produce immutable fixture-only
  canonical observation/version/lineage/stage/candidate records without
  inferring unavailable evidence.
- F-005: Default-disabled capture preserves existing Selection and shared
  consumer behavior; no runtime path receives an implicit sink or observer.
- F-006: Phase 1C-1 remains separate from labels, source observation,
  backfill, snapshots, model training, UI, and production activation.

## Implementation Plan

1. Add the additive Phase 1C migration and typed capture models/repository.
2. Implement the capture-batch request, acquire/recover, terminal, membership,
   and durable-gap operations with transaction-bound PostgreSQL validation.
3. Replace the trace-outbox fixture admission validator with the persisted
   validator and add the fixture-only observation capture writer.
4. Add deterministic unit tests and DEV-DB rollback/readback tests after a
   separately authorized DEV-DB migration application.
5. Keep the default null sink and run shared-consumer regression coverage
   through CI/nightly before any future opt-in activation.

## Verification Plan

- Pure state-machine, request-hash, lease/fencing, immutable version, and gap
  separation tests.
- PostgreSQL DEV-DB rollback/readback tests only after explicit DEV-DB DDL
  authorization. Connection values are read exclusively from `.env`.
- Existing Selection, prospective evidence, multi-alpha, Paper, and simulation
  parity is delegated to CI/nightly; this slice keeps the default null sink.
- `python scripts/aistock_feature_workflow.py validate --design
  docs/architecture/advisory_phase1c_capture_foundation_f2_design_20260713.md
  --tier F2` before PR/merge.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | capture batch models, repository, additive migration | state, stale-token/version, no-fork recovery, terminal, membership-seal tests | verified_local | none |
| F-002 | persisted `TraceAdmissionValidator` and trace outbox repository | transaction-lock, disabled-binding, expired-lease, stale-token, exact-retry tests | verified_local | none |
| F-003 | durable gap repository and reconciler handler | LOST versus WRITE_FAILED idempotency, observation failure gap, and no-replay tests | verified_local | none |
| F-004 | observation capture writer and append-only repository | final-payload hash, next-trading-day, no-fork revision, canonical stage/candidate, multi-Alpha exclusion provenance, DEV-DB L4 | verified_local | none |
| F-005 | explicit null-sink default and opt-in wiring boundary | Selection sidecar null-sink/no-throw tests; broader shared-consumer parity delegated to PR CI/nightly | verified_local | none |
| F-006 | package boundaries and migration/runtime separation | forbidden-import, no-observer, no-label/snapshot, no-runtime-DDL tests | verified_local | none |

## Current Implementation Evidence

The matrix above remains the merge acceptance contract and is deliberately not
advanced before all required gates are complete. Current local implementation
evidence is: capture request/attempt state-machine code, persisted PostgreSQL
repository and trace-admission code, fixture-only observation capture, additive
migration plus rollback, `35 passed` focused local tests, `1 passed` rollback-only
DEV-DB L4, L0 pass, and feature-design validation pass. Broader shared
Selection/Paper/simulation parity remains delegated to PR CI/nightly because
the default runtime sink is unchanged and no shared consumer is wired here.

## Risks

- Capture rows and trace envelopes can be large. The frozen capture policy
  limits candidates and bytes before a writer creates any durable row.
- A stale worker must never write after lease takeover. Fencing and database
  row locks are the enforcement boundary; application-side clocks alone are
  insufficient.
- The existing trace payload may not contain every Phase 1 canonical identity
  field. The writer records a durable gap or PARTIAL result rather than
  constructing identity from mutable latest records.

## Rollout And Rollback

- Rollout is limited to DEV/test migration application and fixture-only tests
  after explicit authorization. The production migration remains pending.
- Runtime trace capture remains disabled after merge. No scheduler, observer,
  or background worker is registered by this slice.
- Rollback disables any future explicit Phase 1C opt-in. Append-only capture,
  gap, and observation evidence remains retained and is never rewritten.

## Production Gates

- `production_ddl_gate`: pending; this slice may include an additive migration
  but production application requires a separate user authorization.
- `production_frontend_dependency_gate`: noop.
- `production_backend_dependency_gate`: noop.
- Runtime activation: noop; merge does not enable trace capture, start an
  observer, write historical data, or change any live/simulation process.

## DESIGN-COMPLIANCE-001

- [x] The implementation accepts only explicit `DB_HISTORICAL`,
  `ADVISORY_RUN`, `HISTORICAL_RESEARCH_ONLY`, and `execution_prohibited=true`
  trace identity.
- [x] Capture request identity and recovery attempt identity are separated;
  retries cannot silently create a second attempt.
- [x] Lease expiry, stale fencing, terminal state, missing plan, divergent
  binding, malformed trace, and component-evidence gaps fail with typed reason
  codes rather than fallback data.
- [x] Default Selection, Paper, simulation, QMT, and broker paths are not
  wired to the new sidecar.
- [x] DEV-DB apply/readback/rollback L4 proves calendar adjacency, revision
  predecessor, immutable state, persisted admission, membership seal, and
  cleanup. Shared-consumer parity remains delegated to PR CI/nightly without
  changing the default null-sink runtime boundary.
