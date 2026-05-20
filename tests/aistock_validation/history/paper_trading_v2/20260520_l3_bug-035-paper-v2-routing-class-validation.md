# BUG-035 Paper v2 daemon routing_class validation

- Module: paper_v2 / qe_archive
- Level: L3 backend integration + L0 guardrails
- Date: 2026-05-20T18:45:37Z
- Branch: `bug/BUG-035-paper-v2-routing-class`
- Worktree: `F:/Dev/AIstock_worktrees/bug-035-paper-v2-routing-class`
- GitHub issue: `https://github.com/licong01-cloud/AIstock/issues/123`
- Code fix commit already in `origin/main`: `91643f7` (`feat(paper-v2): T13 - daemon emit adds payload routing_class (archive/telemetry)`)
- Operator: Codex App

## Scope

- Code was already present in `origin/main` before this BUG-035 registry-sync branch. This branch records source-of-truth BUG/GitHub linkage and validation evidence.
- Runtime files covered by validation: `backend/services/paper_trading_v2/daemon/event_log.py`, `backend/services/qe_archive/handlers/paper_v2_archive_handler.py`, `backend/services/qe_archive/handlers/contract.py`.
- Test files covered by validation: `backend/tests/paper_trading_v2/test_daemon_pg_outbox.py`, `backend/tests/paper_trading_v2/test_daemon_outbox_dev_db.py`, `backend/tests/qe_archive/test_paper_v2_archive_handler.py`, `backend/tests/qe_archive/test_handler_contract.py`.
- Out of scope: production backend `8001`, frontend `3000`, production DB, archive worker enablement, MiniQMT runtime, and strategy assets.
- Protected assets reviewed: no StrategyPackage manifest/model/factor asset, Paper v2 ledger, QE archive production table contents, or MiniQMT state was modified.

## DESIGN-COMPLIANCE-001 Matrix

| Design / closure item | Implementation refs | Test / evidence | Status | Gap or exception |
|---|---|---|---|---|
| `paper.daemon.*` payloads carry `routing_class=telemetry` | `backend/services/paper_trading_v2/daemon/event_log.py:108`, `backend/services/paper_trading_v2/daemon/event_log.py:472` | `test_emit_daemon_event_has_routing_class_telemetry`, `test_emit_all_9_daemon_events_get_telemetry`, dev DB `test_daemon_routing_class_telemetry` | Pass | None |
| Archive capture triggers carry `routing_class=archive` | `backend/services/paper_trading_v2/daemon/event_log.py:94`, `backend/services/paper_trading_v2/daemon/event_log.py:108` | `test_archive_events_get_routing_class_archive` in `backend/tests/paper_trading_v2/test_daemon_pg_outbox.py` | Pass | None |
| Unknown event types fail fast instead of silently routing | `backend/services/paper_trading_v2/daemon/event_log.py:108` | `backend/tests/paper_trading_v2/test_daemon_pg_outbox.py` routing-class tests passed | Pass | None |
| `PaperV2ArchiveHandler.can_handle()` rejects telemetry | `backend/services/qe_archive/handlers/paper_v2_archive_handler.py:102`, `backend/services/qe_archive/handlers/paper_v2_archive_handler.py:121` | `test_rejects_paper_daemon_telemetry`, `test_rejects_non_archive_routing` | Pass | None |
| Shared archive handler contract rejects non-archive routing classes | `backend/services/qe_archive/handlers/contract.py:172` | `backend/tests/qe_archive/test_handler_contract.py::TestCanHandleRoutingClass` => 5 tests inside group passed | Pass | None |
| No schema-column drift introduced; routing remains payload-based | Existing tests query `payload->>'routing_class'`; no migration/DDL touched | dev DB routing test passed; changed files are registry/evidence only in this branch | Pass | None |
| No simplified / POC-only delivery | Code is production daemon/outbox path already in main; validation covers fake-PG, dev DB, archive handler gate, Paper v2 backend and QE archive backend suites | commands below | Pass | None |
| Production safety boundary preserved | Work ran in isolated worktree; no service restart or production DB write except scoped dev DB test cleanup under `test_int5_%` source_id | command log and test fixture cleanup | Pass | None |

## Commands and Results

```bash
python -m pytest backend/tests/paper_trading_v2/test_daemon_pg_outbox.py -q -p no:cacheprovider
```

Result: `20 passed in 1.14s`.

```bash
python -m pytest backend/tests/paper_trading_v2/test_daemon_outbox_dev_db.py::test_daemon_routing_class_telemetry -q -p no:cacheprovider
```

Result: `1 passed in 1.02s`.

```bash
python -m pytest backend/tests/qe_archive/test_paper_v2_archive_handler.py::TestCanHandleAndValidate::test_rejects_paper_daemon_telemetry backend/tests/qe_archive/test_paper_v2_archive_handler.py::TestCanHandleAndValidate::test_rejects_non_archive_routing backend/tests/qe_archive/test_handler_contract.py::TestCanHandleRoutingClass -q -p no:cacheprovider
```

Result: `7 passed in 0.32s`.

```bash
python -m nox -s paper_v2_backend
```

Result: `443 passed, 1 skipped, 2 xfailed in 16.64s`; nox session successful.

```bash
python -m nox -s qe_archive_backend
```

Result: `106 passed in 9.57s`; nox session successful.

```bash
python -m compileall backend/services/paper_trading_v2/daemon/event_log.py backend/tests/paper_trading_v2/test_daemon_pg_outbox.py backend/tests/paper_trading_v2/test_daemon_outbox_dev_db.py backend/tests/qe_archive/test_paper_v2_archive_handler.py backend/tests/qe_archive/test_handler_contract.py
```

Result: PASS.

```bash
git diff --check
```

Result: PASS.

```bash
python -m nox -s guardrail_changed_files -- --changed-only
```

Result: nox successful; `files=1, findings=0, blocking=0`; module ownership `files=1, mapped=1, unmapped=0, ambiguous=0`.

```bash
python -m nox -s validation_module_registry_l0
```

Result: `8 passed in 0.99s`; nox session successful.

## Business Outcomes Verified

- Paper v2 daemon outbox payload now carries `routing_class=telemetry` for all 9 daemon lifecycle events.
- Archive trigger events are mapped to `routing_class=archive`.
- QE archive handler rejects telemetry through `can_handle()` and payload validation; telemetry cannot be archived accidentally by bypassing event-type filtering alone.
- The dev DB reproduce path for `payload->>'routing_class'` passes and performs scoped cleanup under the test source-id prefix.

## Residual Risks

- Archive worker enablement smoke was not run; BUG-035 closure only verifies payload taxonomy and handler gate. Worker-enable smoke remains an operational activation step.
- GitHub issue #123 was created after the code fix was already in main, so this branch is registry/evidence synchronization rather than new code delivery.
- Production backend/frontend/runtime were not restarted or touched.

## Final Result

- Validation status: PASS.
- BUG-035 can move from `in_progress` to `fixed` and GitHub issue #123 can be labeled `status:fixed` / closed as completed.
- Production impact during validation: none.
