# BUG-473 QE Archive outbox telemetry blackhole plan (2026-06-22)

## 0. Scope and decision state

This is the plan-only artifact for BUG-473 / GitHub #1461. It assumes BUG-471 / PR #1457 is already merged and does not rework the archive routing isolation that BUG-471 delivered.

Current live read-only probe on 2026-06-22 returned `GET /api/v1/qe-archive/health = 200`, but the payload still had only the pre-BUG-471 health fields: `pending_outbox_count=21446`, `outbox_status_counts={pending:21446}`, no `pending_archive_outbox_count`, no `outbox_source_routing_counts`, and no `outbox_oldest_pending`. This means the running backend has probably not loaded the merged BUG-471 health additions yet. No restart was performed in this plan phase.

Hard gates preserved in this plan:

- no production DB write, DDL, DML, service start, or service restart;
- implementation deferred until Tier2 approves this design;
- module boundary limited to `qe_archive` and `paper_trading_v2/daemon/event_log.py` routing/consumer-side behavior;
- no Research Assistant paths or tables.

## 1. Evidence and root cause

Confirmed from current code and user-provided live evidence:

1. `backend/services/paper_trading_v2/daemon/event_log.py` defines nine high-frequency `paper.daemon.*` event types and maps them to `routing_class='telemetry'`.
2. The same module writes every daemon event directly into `qe_archive.outbox_event` with `source_system='paper_v2.daemon'` and `status='pending'`.
3. `backend/services/qe_archive/worker_service.py` only registers handlers for `qe.loop.completed` and `qe.experiment.completed`. After BUG-471, the worker claims only `routing_class='archive'` rows for registered event types, so telemetry rows are never claimed.
4. `backend/services/qe_archive/handlers/paper_v2_archive_handler.py` exists for three low-frequency paper archive event types, but it is not registered in the production worker. Those rows can also remain pending if they are emitted.
5. The user confirmed `paper_v2` data is throwaway/debug data with no long-term value.

Root cause: `qe_archive.outbox_event` is being used as a durable shared queue for both QE archive work and high-frequency paper daemon telemetry, but production consumers intentionally only process QE archive work. The remaining paper rows are not failed, skipped, audited, or cleaned, so they form a silent durable blackhole.

## 2. Design alternatives for B (root fix)

### Option 1: stop durable outbox writes for paper daemon telemetry by default

Change the paper daemon event logger so `paper.daemon.*` telemetry is not inserted into `qe_archive.outbox_event` by default. Keep local SQLite fallback/audit behavior for daemon lifecycle debug, and make any PG telemetry sink opt-in behind an explicit flag if future debugging needs it. Archive-class paper event constants remain defined and routed as `routing_class='archive'`, but their current production decision is DEFER/SKIP (see section 5).

Implementation shape after Tier2 approval:

- In `event_log.py`, split the PG write path by routing class.
- For `routing_class='telemetry'`, do not call the `qe_archive.outbox_event` insert unless an explicit opt-in flag is enabled.
- Keep SQLite record semantics for daemon local debug; the returned `DaemonEventRecord` still exists and paper trading behavior is unchanged.
- Update `replay_unsynced_on_startup()` so old SQLite unsynced telemetry is not replayed into `qe_archive.outbox_event`; it should mark/report telemetry skip counters explicitly rather than re-polluting the archive queue.
- Keep fail-fast for unknown event types.

Pros:

- Smallest durable-system change for throwaway paper data.
- No new DB objects and no production DDL.
- Preserves BUG-471/T13 archive routing: durable archive work still uses `routing_class='archive'`; telemetry is simply not archive queue work.
- Prevents new telemetry backlog at source, instead of relying on cleanup forever.

Risks / mitigations:

- Some tests may currently assert telemetry PG insert. Update them to assert local SQLite debug persistence and explicit no-PG telemetry behavior.
- If future ops wants paper daemon telemetry in PG, the opt-in flag must be documented as debug-only and bounded.
- If `replay_unsynced_on_startup()` is missed, SQLite backlog could reinsert old telemetry. Add a targeted test.

### Option 2: keep same outbox table and add telemetry reaper

Leave paper daemon telemetry writes in `qe_archive.outbox_event`, then add a routing-class-aware reaper that periodically marks/deletes/skips telemetry rows and audits reason codes.

Pros:

- Minimal change to the current writer contract.
- Can clean existing pending rows and future rows using the same mechanism.

Risks:

- Keeps using a durable archive queue for non-archive telemetry, so queue growth is expected whenever reaper is delayed.
- Adds a permanent cleanup dependency for data the user says has no durable value.
- Still leaves `qe_archive.outbox_event` health noisy unless the reaper is continuously running.
- Higher chance of future queue pollution or head-of-line symptoms when the reaper is misconfigured.

## 3. Recommendation

Recommend Option 1 as the core B fix: stop writing `paper.daemon.*` telemetry into `qe_archive.outbox_event` by default, and make any PG telemetry path opt-in/debug-only.

Rationale:

- Paper daemon telemetry is confirmed throwaway; durable archive outbox is the wrong sink.
- BUG-471 already established `routing_class='archive'` as the worker claim boundary; this plan keeps that contract clean.
- A reaper-only fix treats the symptom and keeps the queue polluted by design.
- No DDL is needed for the root fix, which reduces production activation risk.

A bounded independent telemetry sink/table is not recommended for this round because it requires new DDL for throwaway data. If future paper observability becomes product-critical, add it later as a separate design with retention, volume limits, and owner-defined value.

## 4. A: scoped stop-bleeding cleanup SQL (manual DML only)

Deliver `docs/analysis/qe_archive_paper_telemetry_outbox_cleanup_20260622.sql` as a manual, gated script. The script only targets rows matching all four predicates:

```sql
source_system = 'paper_v2.daemon'
payload->>'routing_class' = 'telemetry'
status = 'pending'
event_type LIKE 'paper.daemon.%'
```

It does not touch any `qe.*` events and does not touch any `paper_v2` source tables.

Operational policy:

- The operator must run the pre-count and event-type breakdown first.
- If the count or breakdown does not match the expected incident scope, stop.
- The forward cleanup is a single transaction deleting only those telemetry pending rows and returning deleted count/time bounds.
- Verification expects zero remaining rows for the scoped predicate.
- Rollback is possible only before commit. After commit, no reconstruction is planned because telemetry has no durable value; the deleted count and time bounds are the audit record.

This plan phase does not execute the SQL.

## 5. D/E: no-silent-error and paper archive decision

### Explicit decision for low-frequency paper archive events

DEFER production registration of `PaperV2ArchiveHandler` for this round.

Reason:

- User confirmed paper_v2 is throwaway/debug data.
- Registering the handler now would activate a broad mirror into many `qe_archive.paper_v2_*` tables, which is not justified for this incident.
- The immediate objective is to protect QE archive and eliminate blackhole behavior, not to make paper archive first-class.

### Required no-silent-error behavior after Tier2 approval

Rows that cannot or will not be processed must become explicit terminal/audited outcomes, not invisible pending rows.

Planned behavior:

1. `paper.daemon.*` telemetry should not enter durable outbox going forward.
2. Existing pending paper telemetry is cleaned by the manual DML script in section 4.
3. Any remaining/future paper low-frequency archive rows should be explicitly skipped with a reason code, not left pending.
4. Unknown/unhandled outbox rows within the worker-visible boundary should produce loud, specific reason metadata.

Recommended reason codes:

- `paper_daemon_telemetry_not_archived`: paper daemon telemetry is debug-only and not durable archive work.
- `paper_v2_archive_deferred_throwaway`: paper low-frequency archive event was intentionally deferred because paper_v2 is throwaway.
- `unsupported_outbox_event_type`: an event reached the worker/reaper but no handler/policy exists.

Audit target:

- `qe_archive.skip_registry`: `archive_policy='SKIP'`, `archive_policy_source='paper_v2_throwaway_policy'`, `skip_reason=<reason_code>`, source fields copied from outbox.
- `qe_archive.ingest_history`: `ingest_status='skipped'`, `archive_policy='SKIP'`, and `stats.reason_code=<reason_code>`.
- `qe_archive.outbox_event`: terminal status should be explicit. Preferred implementation is a repository helper such as `skip_outbox_event(event_id, reason, stats)` that sets `status='completed'` plus an `error_message`/stats reason and writes skip/history in one transaction. If reviewers consider `completed` misleading, use `failed` with a non-retryable reason; do not leave `pending`.

Note: current `ingest_history.trigger_reason` CHECK allows `realtime`, `backfill`, `retry`, `manual`, and `rebootstrap`. To avoid DDL in this bug, use `trigger_reason='manual'` for the gated cleanup/reaper path and `trigger_reason='realtime'` for worker-time policy skips. Do not add a new `reaper` enum in this round unless Tier2 requests a DDL-bearing design.

## 6. Impact on dw-foundation T6.2/T13 routing contract

- T13 routing is preserved for archive work: every durable archive payload that should be processed by QE archive remains `routing_class='archive'`.
- `paper.daemon.*` remains semantically `routing_class='telemetry'`, but telemetry is no longer represented as durable archive queue work by default.
- This intentionally narrows the T6.2 interpretation: PG `qe_archive.outbox_event` is not the primary durable sink for every daemon lifecycle event; it is the archive work queue.
- The change reduces blast radius for QE archive and keeps the routing contract aligned with consumer capability.

## 7. Implementation plan after Tier2 approval

1. Add/adjust tests for `event_log.py`:
   - daemon telemetry does not insert PG outbox by default;
   - SQLite local record still exists;
   - explicit opt-in, if implemented, is gated and documented;
   - replay does not push unsynced telemetry into `qe_archive.outbox_event`.
2. Add QE archive skip/reaper tests:
   - paper low-frequency archive rows are skipped with `paper_v2_archive_deferred_throwaway`;
   - skip writes `skip_registry` and `ingest_history`;
   - rows do not remain `pending`;
   - unsupported event type is loud.
3. Implement the smallest code path that passes those tests:
   - `event_log.py` telemetry PG suppression / replay guard;
   - a bounded `qe_archive` policy-skip helper/reaper or worker-adjacent service for paper rows;
   - repository transaction helper if needed.
4. Keep `PaperV2ArchiveHandler` unregistered and document the deferral in code comments/tests.
5. Run required validation:
   - l0;
   - qe_archive_backend;
   - qe_archive_l3 with `QE_ARCHIVE_L3_SKIP_UI=1`;
   - targeted `paper_trading_v2` and `qe_archive` tests;
   - ruff/py_compile;
   - frontend only if UI files change.

## 8. Open Tier2 review questions

1. For terminal outbox status on policy skips, prefer `completed` with explicit skip audit or `failed` with non-retryable reason?
2. Should the implementation include a debug opt-in PG telemetry flag, or should telemetry PG writes be removed entirely for this daemon?
3. Should the manual cleanup SQL be run before or after backend restart that loads BUG-471, or should it wait until the B/D fix is merged and restarted?
