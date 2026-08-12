# Resource and Worker contract

## Contents

- [Architecture boundary](#architecture-boundary)
- [Hard resource contract](#hard-resource-contract)
- [Pressure ladder](#pressure-ladder)
- [Worker modes](#worker-modes)
- [Lease, fence and orphan recovery](#lease-fence-and-orphan-recovery)
- [Waiting and hard failures](#waiting-and-hard-failures)
- [Logging and performance](#logging-and-performance)

## Architecture boundary

FastAPI and the one-click CLI are control surfaces. They submit/read durable state and never hold a full panel or run a heavy child. Data-bearing work belongs to the independent Worker and task-owned resource supervisor.

Do not start or register a Worker as a side effect of `monthly`, API submission, Skill invocation, backend lifespan or test collection. Source merge, backend activation, Worker service registration and scheduler enablement are separate states and authorizations.

The scheduler is optional and disabled by default. It runs as Worker reconcile mode or an external scheduler, never as FastAPI background work. It only creates candidate intents and never activates production.

## Hard resource contract

Read live values from the allowlisted profile. `qe_hmm_full_v1` v1 freezes these defaults/hard boundaries:

| Resource | Boundary |
|---|---:|
| same-host heavy full concurrency | 1 |
| aggregate owned private commit | 12 GiB |
| Windows-only child Job commit | 8 GiB |
| hybrid/WSL Windows-side Job commit | 4 GiB |
| WSL `memory.high` / `memory.max` / `swap.max` | 6 GiB / 8 GiB / 0 |
| host start/emergency available | 16 GiB / 8 GiB |
| host start/emergency commit headroom | 16 GiB / 8 GiB |
| WSL start/emergency available | 12 GiB / 6 GiB |
| DB pool / simultaneous row-producing query | 4 / 1 |
| DB statement timeout | 300 s |
| provider request concurrency | 1 |
| Qlib dump workers | 8 |
| minute code batch / date chunk | 20 / 3 months |
| H5 compatibility setting / date chunk | 100 (telemetry only) / 3 months |
| Parquet row group / validation read chunk | 100,000 / 100,000 rows |
| enforcement sample / receipt rollup / wait deadline | 1 s / 5 s / 3,600 s |
| candidate free-space reserve | max(32 GiB, 1.25 × predicted new bytes) |

Profiles may reduce concurrency/chunk size or increase reserves. They may not increase a maximum, lower a reserve, enable WSL swap or bypass a limit through CLI/env. Resource contract changes require a versioned policy and comparable benchmark; they do not change data-byte identity.

Every SQL/provider response is bounded before transform. “Load a large response and split it afterward” violates the contract.

## Pressure ladder

Use only the frozen rungs, in order:

```text
H5 configured value:100 -> 50 -> 20 (compatibility telemetry; not an active memory control)
minute batch:       20 -> 10 -> 5
date chunk months:   3 -> 1
row group rows: 100000 -> 50000
dump workers:         8 -> 4 -> 2
```

At a safe checkpoint, a new attempt may select the next rung when resource preflight is otherwise healthy. Never reduce stocks, dates, fields, PIT spans, 12-index scope or validation to fit memory. If the lowest rung still fails, enter typed `WAITING_RESOURCE` or resource block; do not keep allocating.

Factor H5/static memory is actively bounded by one-date-slice processing plus
`row_group_rows`; the historical `h5_batch` profile field is retained only for
v1 receipt/profile compatibility and MUST NOT be reported as an effective
throttling mechanism. Minute manifests are bound again by the parent processor
to the selected `minute_batch` rung.

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

Before any data-bearing Windows child runs, the supervisor creates it suspended, assigns it to the non-breakaway task Job, applies/readbacks limits, then resumes it. WSL heavy work must run in the attempt/fence-bound transient user-systemd cgroup with guardian heartbeat, `KillMode=control-group`, memory high/max, swap zero and membership readback.

If Job/cgroup enforcement or guardian ownership cannot be proved before a source query, return `BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE`. Do not run “with telemetry only.”

## Lease, fence and orphan recovery

Build claims atomically acquire, in fixed order:

```text
host:heavy-dataset
release:<release_id>
```

Each attempt stores separate host and release fencing tokens. Heartbeat/resource admission checks the host token; candidate checkpoint/publish checks attempt plus release token. Every checkpoint, append, CAS ref and publish revalidates the applicable tokens.

Lease reclaim requires expiry and complete parent/Windows-child/WSL-child quiescence. PID plus create time prevents PID-reuse mistakes. Any child `alive` or `unknown` produces `WAITING_ORPHAN_QUIESCENCE`; the orphan deadline is an alert, not permission to free a lease.

For stale WSL `ACTIVE` evidence after Windows-owner loss, recovery may only perform a read-only query of the exact
attempt/fence/distro transient unit and its recorded cgroup. Exact inactive/collected-unit plus empty/absent cgroup evidence
creates a fence-bound recovery receipt; command failure, unexpected locale/output, another unit/group, populated cgroup, or
missing identity remains `unknown`. Recovery never stops a unit or kills a process. A second supervisor cannot overwrite an
existing guardian state for the same attempt/fence.

After publish-owner loss, attempt/pointer and both leases enter `ORPHAN_HOLD` atomically. Once the entire tree is proven quiescent, one `FINALIZER_RECOVERY` owner adopts with new fences; there is no intermediate FREE window.

Never recover by killing another process, deleting a lock, clearing a lease row or editing a fence. Task-owned Job/cgroup fail-stop applies only to children assigned at creation and only for hard resource/supervisor-loss enforcement.

## Waiting and hard failures

| State/category | Meaning | Operator action |
|---|---|---|
| `WAITING_RESOURCE` | reserve/free-space pressure before work | leave durable task; recheck later |
| `WAITING_PERFORMANCE_REGRESSION` | comparable workload outside threshold | inspect receipt; do not widen caps |
| `WAITING_SOURCE` | required source not yet ready | wait for source; do not fill silently |
| `WAITING_ORPHAN_QUIESCENCE` | old owned tree alive/unknown | observe only; do not kill/delete |
| resource hard failure | Job/cgroup/OOM/system commit/guardian loss | task-only fail-stop, retain staging/checkpoint |
| provider terminal | 40203/conflict/incomplete canonical session | report pending scope; no retries disguised as success |

Wait time, provider wait and compute time must be recorded separately. Resource waiting may increase wall time without being a compute regression.

## Logging and performance

Stream stdout/stderr to rotated, bounded logs. API/CLI log readers use a real forward cursor over
`(catalog log_id, segment generation, byte_offset)` with byte+line caps. The protected API signs that position together
with principal, endpoint, run, stream, filter and order; the next page resumes from returned
`next_log_id/next_generation/next_byte_offset`, not from a recomputed tail. Never use unbounded `capture_output` or load a multi-hour log into memory.

Each CAS log segment is at most 16 MiB by default; one supervised child has one shared stdout/stderr budget of at most
128 segments (2 GiB at the maximum segment size). One API read returns at most 1 MiB and 1,000 lines. A control-root
capacity watermark blocks new heavy work with `CONTROL_ROOT_CAPACITY_EXCEEDED`; it never authorizes automatic evidence or candidate deletion.

Record per stage/chunk:

- Windows Job current/peak private commit and child RSS/private bytes;
- host available/commit headroom/low-memory/page reads/pagefile;
- WSL cgroup current/peak/high/max/events/swap and guardian identity;
- DB query count/rows/time, provider requests/wait, X-disk read/write;
- rows, bytes, compute seconds, wait seconds and reuse savings.

For a comparable synthetic workload, three-run median compute must be within 110% of baseline and throughput at least 90%, with query/memory thresholds satisfied. `benchmark_not_comparable` is not a PASS for source merge evidence.

Candidate materialization can reuse/COW unchanged component partitions, but full source correctness is a different layer.
Without a trusted DB revision ledger, initial source freeze and prepublish DB-only recheck may each scan all required values
through cutoff. MVCC/watermark reuse is disabled for content equivalence. A future ledger is a separate F2 plus DEV and
target-specific production DDL/DML authorization; it is not silently created by this workflow.

For an authorized production-sized monthly run, a normalized compute regression above 10% is a warning. Sustained for 15 minutes, throughput below 70% of baseline or regression above 30% enters `WAITING_PERFORMANCE_REGRESSION` at a safe checkpoint. Zero progress for 30 minutes, one SQL exceeding the 300-second timeout, or a hard resource breach stops the current attempt. Recovery may move only to the next pressure-ladder rung. Real full-data telemetry remains separately authorized.

Current implementation acceptance is candidate-only and fixture-scoped:

```text
source_state=source_ready_fixture_verified
platform_hard_cap_evidence=pending
runtime_real_data_evidence=not_run_not_authorized
real_full_scale_performance=pending
production_activation=not_requested
```

Do not turn the fixture heartbeat/log/resource receipts into claims about a registered Worker, a real WSL cgroup run or a
production-sized monthly release.
