# Resource and Worker contract

## Contents

- [Architecture boundary](#architecture-boundary)
- [OS-managed execution and telemetry](#os-managed-execution-and-telemetry)
- [Worker modes](#worker-modes)
- [Lease, fence and orphan recovery](#lease-fence-and-orphan-recovery)
- [Waiting and hard failures](#waiting-and-hard-failures)
- [Logging and performance](#logging-and-performance)

## Architecture boundary

FastAPI and the one-click CLI are control surfaces. They submit/read durable state and never hold a full panel or run a heavy child. Data-bearing work belongs to the independent Worker and task-owned resource supervisor.

Do not start or register a Worker as a side effect of `monthly`, API submission, Skill invocation, backend lifespan or test collection. Source merge, backend activation, Worker service registration and scheduler enablement are separate states and authorizations.

The scheduler is optional and disabled by default. It runs as Worker reconcile mode or an external scheduler, never as FastAPI background work. It only creates candidate intents and never activates production.

## OS-managed execution and telemetry

AIstock does not perform resource admission or resource-driven state transitions for monthly dataset work. Profile resource
fields remain readable only for historical receipt compatibility and telemetry; they do not impose host, Job, cgroup, swap,
memory, disk-reserve, paging, concurrency or performance gates.

The operating system, Docker/WSL, PostgreSQL and the filesystem remain the only resource authorities. An actual OOM/process
termination, database error/timeout or ENOSPC ends the current attempt with its original external error. AIstock does not
pre-emptively checkpoint, enter `WAITING_RESOURCE`, automatically lower a pressure rung, or automatically retry a full source
freeze.

Every SQL/provider response is still bounded before transform. One-date H5 slices, code/date batches, Parquet row groups and
bounded validation reads remain algorithmic implementation details that prevent unbounded Python materialization; they are not
admission gates and cannot reduce stocks, dates, fields, PIT spans, 12-index scope or validation.

Historical pressure-rung fields may be emitted as compatibility telemetry, but runtime selection is fixed by the data component
implementation and never changes because of host resource observations.

## Worker modes

Runtime entrypoints:

```powershell
$Profile = (Resolve-Path .\configs\datasets\qe_backtest_monthly_v1.yaml).Path
$ControlRoot = 'X:\AIstock_dataset_release_control'
rtk python scripts/dataset_release_worker.py --preflight --profile $Profile --control-root $ControlRoot
rtk python scripts/dataset_release_worker.py --once --profile $Profile --control-root $ControlRoot
rtk python scripts/dataset_release_worker.py --drain --max-jobs N --profile $Profile --control-root $ControlRoot
rtk python scripts/dataset_release_worker.py --serve --profile $Profile --control-root $ControlRoot
```

- `--preflight` is read-only: no claim, heartbeat, Worker loop or data child. It verifies the existing store, registry,
  clean dependency/code identity and frozen external Qlib toolchain.
- `--once` claims at most one attempt.
- `--drain` handles a bounded count, then exits.
- `--serve` polls continuously using configured interval and resource class.
- An idle `--serve` backs off polling from 5 to 10 to at most 15 seconds. Worker-health heartbeat is persisted on a state
  change or at least every 15 seconds, rather than fsync-writing on every unchanged idle poll. Claimed work is never
  heartbeat-throttled, and a long-running processor has an independent health-heartbeat thread.
- Missing control root/profile allowlist/capability causes fail-closed startup.
- Worker identity includes instance/host/PID/create-time/code SHA/schema capabilities/profile digests.
- SIGINT/SIGTERM requests cooperative shutdown at checkpoints; it does not authorize killing unrelated or orphan processes.
- Exit never deletes candidates, events, lease history, receipt, attestation, checkpoint or failure evidence.

Before any data-bearing Windows child runs, the supervisor may assign it to the non-breakaway task Job for identity, cancellation
and descendant ownership only; it does not apply an AIstock memory/commit limit. WSL transient units/cgroups provide exact
attempt/fence ownership and cleanup identity without AIstock memory.high/max/swap enforcement. Missing ownership identity still
fails closed because it is process-safety evidence, not a resource admission policy.

## Lease, fence and orphan recovery

Build claims atomically acquire, in fixed order:

```text
host:heavy-dataset
release:<release_id>
```

Each attempt stores separate host and release fencing tokens. Ownership heartbeat checks the host token; candidate checkpoint,
append, CAS ref and publish revalidate the applicable tokens.

Lease reclaim requires expiry and complete parent/Windows-child/WSL-child quiescence. PID plus create time prevents PID-reuse mistakes. Any child `alive` or `unknown` produces `WAITING_ORPHAN_QUIESCENCE`; the orphan deadline is an alert, not permission to free a lease.

For stale WSL `ACTIVE` evidence after Windows-owner loss, recovery may only perform a read-only query of the exact
attempt/fence/distro transient unit and its recorded cgroup. Exact inactive/collected-unit plus empty/absent cgroup evidence
creates a fence-bound recovery receipt; command failure, unexpected locale/output, another unit/group, populated cgroup, or
missing identity remains `unknown`. Recovery never stops a unit or kills a process. A second supervisor cannot overwrite an
existing guardian state for the same attempt/fence.

After publish-owner loss, attempt/pointer and both leases enter `ORPHAN_HOLD` atomically. Once the entire tree is proven quiescent, one `FINALIZER_RECOVERY` owner adopts with new fences; there is no intermediate FREE window.

Never recover by killing another process, deleting a lock, clearing a lease row or editing a fence. Job/cgroup ownership applies
only to children assigned at creation and is not a resource gate.

## Waiting and hard failures

| State/category | Meaning | Operator action |
|---|---|---|
| `WAITING_SOURCE` | required source not yet ready | wait for source; do not fill silently |
| `WAITING_ORPHAN_QUIESCENCE` | old owned tree alive/unknown | observe only; do not kill/delete |
| actual OS/DB/filesystem failure | preserve the external error and terminalize the attempt | fix the external condition, then explicitly submit again |
| provider terminal | 40203/conflict/incomplete canonical session | report pending scope; no retries disguised as success |

Provider wait and compute time remain separate telemetry. Resource observations never change task state.

## Logging and performance

Stream stdout/stderr to rotated, bounded logs. API/CLI log readers use a real forward cursor over
`(catalog log_id, segment generation, byte_offset)` with byte+line caps. The protected API signs that position together
with principal, endpoint, run, stream, filter and order; the next page resumes from returned
`next_log_id/next_generation/next_byte_offset`, not from a recomputed tail. Never use unbounded `capture_output` or load a multi-hour log into memory.

Each CAS log segment is at most 16 MiB by default; one supervised child has one shared stdout/stderr budget of at most
128 segments (2 GiB at the maximum segment size). One API read returns at most 1 MiB and 1,000 lines. Control-root capacity is
telemetry; only an actual filesystem write failure/ENOSPC ends the attempt and never authorizes deletion.

Record per stage/chunk:

- Windows Job current/peak private commit and child RSS/private bytes;
- host available/commit headroom/low-memory/page reads/pagefile;
- WSL cgroup current/peak/high/max/events/swap and guardian identity;
- DB query count/rows/time, provider requests/wait, X-disk read/write;
- rows, bytes, compute seconds, wait seconds and reuse savings.

Synthetic and real performance comparisons are telemetry only and never block delivery.

Candidate materialization can reuse/COW unchanged component partitions, but full source correctness is a different layer.
Without a trusted DB revision ledger, initial source freeze and prepublish DB-only recheck may each scan all required values
through cutoff. MVCC/watermark reuse is disabled for content equivalence. A future ledger is a separate F2 plus DEV and
target-specific production DDL/DML authorization; it is not silently created by this workflow.

For an authorized production-sized monthly run, normalized compute regression never blocks the release. An actual SQL timeout,
OOM, process termination or filesystem error ends only the current attempt and is not automatically retried. Source-not-ready
states retain their existing provider retry semantics. Real full-data telemetry remains separately authorized.

Current implementation acceptance is candidate-only and fixture-scoped:

```text
source_state=source_ready_fixture_verified
platform_hard_cap_evidence=fixture_platform_verified_real_full_pending
runtime_real_data_evidence=not_run_not_authorized
real_full_scale_performance=pending
production_activation=not_requested
```

Do not turn the fixture heartbeat/log/resource receipts into claims about a registered Worker, a real WSL cgroup run or a
production-sized monthly release.
