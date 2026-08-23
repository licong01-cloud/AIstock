# Monthly workflow

## Authority and readiness

Use these authorities in order:

1. `configs/datasets/qe_backtest_monthly_v2.yaml` for canonical monthly semantics and its inherited storage/resource policy;
   `qe_backtest_monthly_v1.yaml` is read only for explicit historical reproduction/re-attestation.
2. SQLite control catalog plus immutable CAS for submissions, runs, events, receipts and attestations.
3. Candidate committed marker and artifact hashes for a published release.
4. CLI/API bounded projections for operator display.

Do not infer runtime readiness from merged source. Control-store initialization, backend route activation, Worker registration, operator token configuration and scheduler enablement are separate runtime states. A monthly request never authorizes any of them.

The real control store must be explicitly initialized or migrated by its dedicated administration entrypoint. Runtime CLI, API and Worker must validate schema/capabilities and fail closed; they must not auto-create or auto-migrate it.

Exact runtime-owner commands live in `docs/operations/qe_backtest_dataset_monthly_update_runbook.md`.
Use `dataset_release_control_store.py status` and the zero-execution Worker `--preflight` before any separately
authorized Worker registration/start.

## Ordinary monthly update

Run once:

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 monthly --candidate-only
```

## Fixed first PIT v2 migration

The first PIT v2 migration is not an arbitrary-cutoff monthly request. It accepts exactly one checked-in plan id and
persists the canonical plan digest in submission, immutable build inputs, candidate manifest and terminal receipt:

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration --plan pit_v2_initial_20260731_v1 --scope sample --candidate-only
```

The plan freezes cutoff `2026-07-31`, five stock instruments, event windows and the 12-index boundary oracles. The
resolution child filters the five stock codes before row materialization; the sample PIT binding is validation-only and
cannot drive QE/training. A full intent uses the same plan id/digest and requires separate real-data authorization after
the sample receipt passes. Neither command activates production or overwrites the historical 2026-07-31 v1 candidate.

Defaults come from the live profile:

```text
profile=qe_hmm_full_v2
cutoff=previous_month_last_completed_trading_day
reuse=auto
resume=auto
sample_policy=on_contract_change
activation=not_requested
node1=not_requested
db_repair=not_requested
restart=not_requested
cleanup=not_requested
```

The command creates or links one durable submission. It does not mean a run already exists: source resolution may still be pending, may link an existing validated release, or may terminate before run creation.
Its response returns a random invocation `idempotency_key`; explicitly reuse that key only to retry the same call.

Then use bounded status:

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
```

Do not loop by resubmitting `monthly`. A later manual invocation intentionally creates a new submission/fresh probe;
resolution must link an equivalent existing run/release or produce a fresh no-op rather than duplicate data work.
Only the same explicit idempotency key replays the original response; the same key with a different payload is a conflict.

Bounded signoff/diagnostics:

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 events --run-id <run_id> --limit 50
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 receipt --run-id <run_id>
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 log --run-id <run_id> --max-bytes 262144 --max-lines 1000
```

Continue a log page only with the returned `next_log_id/next_generation/next_byte_offset`. This is a real forward byte
cursor across bounded segments, not a request to recompute the latest tail.

## Same cutoff and NO_OP

Do not call an exporter merely because the cutoff already exists.

1. Resolution discovers the cataloged candidate/release.
2. A fresh source/PIT content probe verifies the candidate identity, artifact root and exact validation identity.
3. If unchanged and current-source-equivalent, the control catalog atomically creates a terminal `SUCCEEDED/NO_OP_VERIFIED` run and links the submission.
4. If the probe TTL is stale or missing, create/reuse one `SOURCE_REVISION_PROBE`; do not reuse old freshness evidence.
5. If bytes are valid but source changed, form a selective rebuild plan rather than no-op.

`NO_OP_VERIFIED` is work avoided with fresh evidence. It is not “directory exists,” “max date matches,” or “previous run passed.”

## Mixed materialization truth

A non-no-op monthly run may mix `REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD` by component and partition.
Daily/minute incremental authority remains immutable, strictly ordered per-instrument CSV segments; selective repair requires
explicit active-segment override evidence. Large writer targets use single-copy deferred COW: the private writer receives at
most one baseline copy and the trusted parent performs a same-volume atomic rename with no final recopy. Minute source and
dump-update code batches are split before materialization at the active rung, never above 20 codes.

Historical `stk_limit` holes inside canonical PIT v2 are never represented as NaN or untradable. Artifact-ready resolution
calculates missing keys and completes partial keys from the versioned exchange/board/date rule, raw previous close and
adjustment-factor ratio. A partial DB row is completable only when every non-null field equals the derived value at cent
precision; a complete row remains authoritative. Its exact affected-instrument list drives full-history overrides only for
those instruments. Unknown/no-limit days, missing price/adjustment inputs, non-null conflicts and unresolved internal keys
block resolution. No DB row, existing candidate or production pointer is changed.

Source-readiness uses three outcomes. Legal sparse empties, exact index candidate fill, limit completion and strict D/P
terminal daily suffixes continue automatically. Provider rate/network/publication failures are retryable and retain the
durable intent. Only authority conflicts, PIT/identity corruption, internal required gaps, missing deterministic inputs and
safety/authorization violations are hard blockers. A new hard blocker must be analyzed and explicitly approved by the user
before implementation; tests cannot introduce a new gate by implication.

These materialization savings do not weaken source truth. Without a trusted revision ledger, source freeze and prepublish
DB-only recheck may still scan all required values through cutoff. Fixture coverage does not prove real full-scale runtime.

## Catalog and re-attest an existing candidate

Use cataloging to register an allowlisted immutable candidate identity. Registration never makes it production and never establishes current-source equivalence by itself.

For the latest eligible cataloged candidate:

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v1 reattest-existing --latest
```

Re-attestation:

- reads the candidate only;
- writes a separate attestation/receipt under control CAS/catalog;
- never adds files to, rewrites, renames, moves, copies or repacks the candidate;
- uses the current full validator and frozen current source/PIT identity;
- distinguishes artifact validity from current-source equivalence.

Required outcomes:

| Evidence | Outcome | Reusable for current monthly release |
|---|---|---|
| artifact invalid | `INVALID` | no |
| artifact valid, current source changed | `ARTIFACT_VALID_SOURCE_CHANGED` | no; selective rebuild |
| artifact valid, provenance insufficient | `ARTIFACT_VALID_ONLY` | no; historical reproduction only |
| original source missing, full current-source value parity passes and PIT provenance is valid | `CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED` | yes, bound to new attestation |
| artifact/source/PIT/current validator all equal | `CURRENT_SOURCE_EQUIVALENT` | yes |

Missing PIT provenance cannot be reconstructed into a reusable current release; it remains artifact-only/blocked legacy provenance.

## Resume and cancel

Automatic resume is the default for a matching non-terminal lineage and checkpoint. Resume must bind the original run, checkpoint digest and monotonically increasing resume ordinal. Different idempotency keys must not create two active resume generations.

Use only the CLI/API action advertised for the current typed state. Never “resume” by appending to a final candidate, deleting a lock, changing SQLite rows, copying staging files or rerunning a low-level exporter.

Cancellation is a durable request:

- before run creation it targets the submission;
- after resolution it targets the run;
- the Worker exits at a safe stage/chunk checkpoint;
- after the publish commit point it becomes `REJECTED_TOO_LATE` and the same run must finish/recover publication;
- cancellation does not authorize process termination or staging deletion.

## Status and API mapping

The protected API is an alternative view/control surface, not a second executor:

```text
POST /api/v1/dataset-releases/preview
POST /api/v1/dataset-releases/runs
GET  /api/v1/dataset-releases/submissions/{submission_id}
GET  /api/v1/dataset-releases/submissions/{submission_id}/events
POST /api/v1/dataset-releases/submissions/{submission_id}/cancel-request
GET  /api/v1/dataset-releases/runs
GET  /api/v1/dataset-releases/runs/{run_id}
GET  /api/v1/dataset-releases/runs/{run_id}/events
GET  /api/v1/dataset-releases/runs/{run_id}/log
GET  /api/v1/dataset-releases/runs/{run_id}/receipt
POST /api/v1/dataset-releases/runs/{run_id}/resume
POST /api/v1/dataset-releases/runs/{run_id}/cancel-request
```

All endpoints require dataset-release operator authentication; write endpoints also require an idempotency key. Durable
mutation rows record actor. Read-only polling binds the authenticated principal to signed cursors but does not create an
access-audit write per GET, avoiding SQLite write amplification. API requests accept only allowlisted profile/scope/candidate
intent, never shell, env, arbitrary roots or production paths. A missing Worker leaves
`submission_state=QUEUED_RESOLUTION` with non-healthy `worker_health.state`; FastAPI must not run the job in
`BackgroundTasks`, lifespan or a daemon thread.

## Advanced diagnostics

`plan`, `run`, `reuse`, `fetch-overlay` and `verify` may exist as diagnostic subcommands. Use them only when a typed failure calls for their exact evidence. They cannot become required steps for an ordinary month and cannot bypass the catalog, source stream, lease, resource supervisor, validator or publisher.

The legacy direct exporter guide is formula/compatibility reference only. Do not fall back to manual daily/minute/H5 commands when the durable workflow is blocked.

Current implementation-round ceiling is:

```text
source_state=source_ready_fixture_verified
mixed_daily_minute_factor_direct_e2e=fixture_verified
selective_override_clean_full_equivalence=fixture_verified
platform_hard_cap_evidence=fixture_platform_verified_real_full_pending
runtime_real_data_evidence=not_run_not_authorized
real_full_scale_performance=pending
production_activation=not_requested
```

This remains candidate-only; it does not authorize reading or rebuilding an older candidate to manufacture acceptance evidence.
