# [VERIFY] Claude paper-v2 5-layer verify of Codex paper_v2 coldstart sanity gate (c2352a9)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex + strategy session
> **date**: 2026-05-11
> **target**: codex/qe-governance-integration-20260509@c2352a9 "feat(qe): add paper v2 cold-start sanity gate"
> **sister verifies**: c2ef5f5 (2fb81b3 strategy_package, READY), 94242c1 (2866f66 protected_asset_ledger, READY)
> **reviewer branch**: claude/paper-v2-baseline-post-r5-20260511 (HEAD 94242c1)
> **SLA**: <=60 min (9:30 cutover tomorrow)

## §0 Verdict

**READY-WITH-CAVEATS**

Per-layer:
- L1 Static (5-guard + audit + per-phase txn): **PASS**
- L2 Tests (30 expected): **PASS** (30/30)
- L3 5 Phases (preflight/sentinel/audit/cleanup/verdict): **PASS** (Phase 2 sentinel params exact match)
- L4 Semantic + runbook §8.5: **PASS** (all 5 fields aligned)
- L5 Sentinel endpoint `/paper-v2/coldstart-sanity/sentinel-order` real existence: **CAVEAT (not BLOCKER)**

Sanity gate readiness for 9:30: **GO-WITH-CAUTION**

Combined with sister c2ef5f5 + 94242c1 (both READY): **R6 cutover sequence still GO**, provided the sentinel endpoint (or an equivalent daemon entry point) is implemented as part of R6 merge / runtime activation before the gate is invoked in --mode=prod. The gate is post-merge tooling per Codex's own caveat; this is documented and expected.

## §1 Scope + commit anchors

Codex commit `c2352a9` "feat(qe): add paper v2 cold-start sanity gate" (4 files, +1473 lines):

| File | LOC |
|---|---|
| `scripts/paper_v2_coldstart_sanity.py` | 845 |
| `backend/tests/scripts/test_paper_v2_coldstart_sanity.py` | 487 |
| `docs/operations/r6_prod_apply_runbook_20260511.md` | +71 (existing 948-line doc — §8.5 added) |
| `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/paper_v2_coldstart_sanity_dry_run.json` | 70 |

Range: `2866f66..c2352a9` = exactly one commit between sister 94242c1 verify and this one.

## §2 Layer 1 — Static review (5-guard + audit + txn)

`_require_prod_guards()` at `scripts/paper_v2_coldstart_sanity.py:240-253` enforces ALL guards BEFORE any DB connection or HTTP call.

5-guard chain (fail-fast, in order):

1. **Token literal** (`scripts/paper_v2_coldstart_sanity.py:242`):
   `_require(args.confirm_prod == CONFIRM_PROD, f"--mode=prod requires exact --confirm-prod {CONFIRM_PROD}")`
   Token = `RUN_PAPER_V2_COLDSTART_SANITY_PROD` (constant at L33).
2. **Env enabled flag** (L243):
   `_require(_env_truthy(ENV_PROD_ENABLED), ...)` -> `AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED=true` (L34).
3. **Env mutex flag** (L244):
   `_require(_env_truthy(ENV_MUTEX_HELD), ...)` -> `AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD=true` (L35).
4. **Non-trading-hours check** (L245):
   `_require(not _is_a_share_trading_window(now), "cold-start sanity refuses A-share trading hours 09:30-15:00 CST on weekdays")`. Helper at L231-237: weekday + `dt_time(9,30) <= local_time <= dt_time(15,0)` (conservative — includes lunch break).
5. **Operator typed confirmation** (L246-248):
   - confirmation non-empty
   - must contain exact `CONFIRM_PROD` token
   - must reference target DB label OR DB name OR `target=prod`

Additional production-target guards (L249-253): `target_db=='prod'`, `port==5432`, `dbname not in {aistock_dev,dev,test}`, host explicit (not empty / dev-loopback).

**Fail-fast ordering verified**: `run_prod()` calls `_require_prod_guards()` at L704 BEFORE any `_check_backend_health` (L707), `_connect()` (L720), or `_trigger_sentinel_order` (L731). If any guard raises `ColdStartSanityError`, the `try/except` wrapper at `main()` L836 emits a NO-GO failure payload and exits 2 with NO DB connection opened, NO HTTP touched, NO sentinel triggered.

**Audit emit**: every phase appends a structured dict via `_phase()` (L285). `_emit()` at L798 ALWAYS writes the JSON report (to `--output` file when given, plus stdout when `--json`) — unconditional, even in failure path via `_failure_payload()` (L808).

**Per-phase transaction / rollback**: `_cleanup_sentinel()` (L577-613) wraps EACH table's `DELETE` in its own `try` block with `conn.commit()` on success and `conn.rollback()` on exception, then returns early with `FAIL`. Per-table commit-or-rollback semantics confirmed; no batch transaction that could orphan partial cleanup state.

**SQL parameterized + table whitelist**: all `cur.execute()` calls use `%s` parameter binding. The `REQUIRED_TABLES` (L46-56) and `CLEANUP_TABLES` (L57-65) are hard-coded constants, not from user input. `_preflight_db_checks` uses `to_regclass(%s)` for existence — parameterized.

**No silent fallback**: `ColdStartSanityError` is raised on any guard failure, DB connect failure, HTTP failure, or assertion mismatch. The `_failure_payload` path preserves the error message in the JSON report and emits exit 2.

**Verdict L1: PASS**

## §3 Layer 2 — Unit tests (30 expected)

```
F:\Dev\AIstock_worktrees\qe-governance-integration-20260509> python -m pytest backend/tests/scripts/test_paper_v2_coldstart_sanity.py -v -p no:cacheprovider
...
backend/tests/scripts/test_paper_v2_coldstart_sanity.py::test_source_does_not_import_dev_backfill_apply_scripts PASSED [100%]
============================= 30 passed in 0.16s ==============================
```

All 30 tests passed. Notable test groups:
- 4 parametrized prod-guard rejection tests (token/env/mutex/operator confirmation each missing -> NO-GO before connect)
- `test_prod_rejects_trading_hours_before_connect` — verifies 09:30-15:00 weekday refusal
- 3 parametrized wrong-DB-target rejection tests (wrong target_db / wrong port / dev DB name)
- `test_preflight_backend_down_stops_before_db` — health check before DB connect
- `test_preflight_daemon_down_stops_before_db` — daemon presence before DB connect
- `test_sentinel_payload_is_exact` — verifies payload `{symbol:000001.SZ, side:BUY, quantity:100, intended_price:"10.00", source:paper_v2_coldstart_sanity}` exactly
- `test_poll_fill_passes_complete_row` + 2 rejection variants (missing market_context, missing timestamps)
- `test_outbox_requires_telemetry_routing` + `test_outbox_allows_pending_or_sent`
- `test_audit_chain_requires_evidence` + `test_audit_chain_rejects_bad_timestamp_order`
- `test_cleanup_commits_each_table` + `test_cleanup_rolls_back_on_failure`
- `test_full_mocked_prod_path_go` — end-to-end happy path with all 5 phases passing
- `test_source_does_not_import_dev_backfill_apply_scripts` — boundary guard

**Verdict L2: PASS (30/30)**

## §4 Layer 3 — 5 Phases

### §4.1 Phase 1 preflight

Source: `run_prod()` lines 707-722.
1. `_check_backend_health(args)` (L707, defined L394-398) — GET `<api_base>/health` and require `status=ok` or `ok=True`.
2. `_check_daemon(args)` (L710, defined L401-408) — `_find_daemon_process(args.daemon_process_name)` via psutil (with `tasklist`/`ps` fallback) and require non-None.
3. `_preflight_db_checks(conn, package_ids)` (L722, defined L299-391) — read-only ping; `to_regclass` existence check for all 9 `REQUIRED_TABLES`; package gates query for `validation_evidence`, `runtime_variant`, `stability_evidence`, `protected_asset_ledger`, `paper_enabled` status.

Dry-run JSON Phase 1: `{"check":"preflight","status":"SKIPPED","message":"dry-run opens no DB connection and makes no HTTP requests"}` (confirmed in my re-run + Codex's archived dry_run.json).

### §4.2 Phase 2 sentinel `000001.SZ BUY 100 ip=10.00`

Constants at L40-43:
```python
SENTINEL_SYMBOL = "000001.SZ"
SENTINEL_SIDE = "BUY"
SENTINEL_QUANTITY = 100
SENTINEL_INTENDED_PRICE = "10.00"
```

`SentinelOrder` dataclass (L98-114) defaults each field to the constants, with `payload()` adding `source="paper_v2_coldstart_sanity"`.

Dry-run JSON Phase 2 verbatim (my re-run, 2026-05-11 22:16 CST):
```json
"sentinel_order": {
  "intended_price": "10.00",
  "quantity": 100,
  "run_id": "sanity-20260511-221634",
  "side": "BUY",
  "source": "paper_v2_coldstart_sanity",
  "symbol": "000001.SZ"
}
```

Codex's archived dry-run (`tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/paper_v2_coldstart_sanity_dry_run.json`) line 58-65: IDENTICAL apart from timestamp/run_id.

**Phase 2 sentinel params EXACTLY match user spec.**

`_trigger_sentinel_order()` (L411-420) POSTs the payload to `args.sentinel_endpoint` (default `/paper-v2/coldstart-sanity/sentinel-order`). See §6 for endpoint existence question.

### §4.3 Phase 3 audit chain

`_check_audit_chain()` (L526-574):
- Reads earliest `strategy_pkg.package_validation_run` row whose `evidence_json::text LIKE '%<run_id>%'`.
- Reads earliest `strategy_pkg.package_asset` row whose metadata or `asset_ref` references the run_id (`governance/coldstart_sanity/<run_id>%`).
- Verifies timestamp ordering: `fill_ts <= evidence_ts <= ledger_ts` (when ledger required).
- Required-ledger gate is parameterized: `--require-ledger-audit` flag (L785) defaults False; runbook §8.5 prod template does NOT enable it explicitly, relying on the upstream protected_asset_ledger executor to populate the row.

`_check_outbox()` (L484-523) requires at least one `qe_archive.outbox_event` row matching the run_id with `payload->>'routing_class'='telemetry'` and `status in {pending,sent}`.

### §4.4 Phase 4 cleanup

`_cleanup_sentinel()` (L577-613). Iterates 7 DELETE statements in order:
1. `qe_archive.outbox_event` (source_id OR payload run_id)
2. `strategy_pkg.package_validation_run` (evidence_json LIKE run_id)
3. `strategy_pkg.package_asset` (metadata LIKE run_id OR asset_ref LIKE governance/coldstart_sanity/<run_id>%)
4. `paper_v2.fills` (run_id)
5. `paper_v2.order_events` (run_id)
6. `paper_v2.orders` (run_id)
7. `paper_v2.run_events` (run_id)

Per-table commit; rollback + early-return on any failure. Cleanup uses a SEPARATE non-readonly connection (L750: `_connect(target, readonly=False)`); the preflight + sentinel-poll + audit connection is read-only (L720). This is the only path that opens a writable conn, and it only fires when `triggered=True` (sentinel POST returned PASS).

### §4.5 Phase 5 verdict

`run_prod()` L760-768:
- `report["cleanup_attempted"]` set
- `report["phases"]` = full check list
- `report["failed_checks"]` = list of FAIL check names
- `report["remedial_action"]` mapped via `_remedial_action()` (L634-647) for each failed check
- `report["status"]` = "passed" / "failed"
- `report["verdict"]` = "GO" / "NO-GO"
- `report["real_trading_ready"]` = boolean (only True when zero failures)

`main()` exit code (L835): 0 on GO, 2 on NO-GO in prod, 0 always in dry-run; 1 on unhandled exception, 2 on `ColdStartSanityError`.

**Verdict L3: PASS** — all 5 phases mapped, sentinel params exact, dry-run skips each phase with explicit message, prod path threads each phase result into the verdict.

## §5 Layer 4 — Semantic + runbook §8.5

Cross-check matrix between `scripts/paper_v2_coldstart_sanity.py` source and `docs/operations/r6_prod_apply_runbook_20260511.md` §8.5 (lines 605-674):

| Field | Source | Runbook §8.5 | Match |
|---|---|---|---|
| CLI invocation (prod) | `python scripts/paper_v2_coldstart_sanity.py --mode prod ...` | L630: `python scripts/paper_v2_coldstart_sanity.py` `--mode prod` | PASS |
| Token | `CONFIRM_PROD = "RUN_PAPER_V2_COLDSTART_SANITY_PROD"` (L33) | L632: `--confirm-prod RUN_PAPER_V2_COLDSTART_SANITY_PROD`; L633 operator-confirmation contains same token | PASS |
| Env enabled | `ENV_PROD_ENABLED = "AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED"` (L34) | L626: `$env:AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED = 'true'` | PASS |
| Env mutex | `ENV_MUTEX_HELD = "AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD"` (L35) | L627: `$env:AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD = 'true'` | PASS |
| Non-trading-hours rule | `_is_a_share_trading_window`: weekday + 09:30-15:00 CST inclusive | L668: "Any phase FAIL ... non-trading-hours check" + L245 error string "09:30-15:00 CST on weekdays" | PASS |
| Operator confirmation phrase template | `_require(CONFIRM_PROD in confirmation, ...)` + target_db/dbname check | L633: `'RUN_PAPER_V2_COLDSTART_SANITY_PROD target=prod packages=<PACKAGE_IDS> approved_by=<RELEASE_COMMANDER>'` — contains token + `target=prod` | PASS |
| Schema version | `SCHEMA_VERSION = "aistock_paper_v2_coldstart_sanity_v1"` (L32) | L654: `"schema_version": "aistock_paper_v2_coldstart_sanity_v1"` | PASS |
| Sentinel payload (BUY 100 @10.00 of 000001.SZ) | L40-43 constants | L657: `{"symbol":"000001.SZ","side":"BUY","quantity":100,"intended_price":"10.00"}` | PASS |

All 8 fields (5 required + 3 incidental) aligned. Abort criteria (L664-669) covers all guard rails. 9:30 gate usage section (L671-674) explicitly states "the production JSON artifact must be attached to the go/no-go evidence package before the 09:29 decision".

**Verdict L4: PASS**

## §6 Layer 5 — ★★★ Sentinel endpoint `/paper-v2/coldstart-sanity/sentinel-order`

The gate's Phase 2 POSTs to `{api_base}/paper-v2/coldstart-sanity/sentinel-order` (default endpoint at L38, configurable via `--sentinel-endpoint`).

### §6.1 Search results in main backend (`F:\Dev\AIstock\backend`)

```
Grep coldstart-sanity|coldstart_sanity|sentinel-order|sentinel_order in F:/Dev/AIstock/backend
-> No files found
```

Router file `F:\Dev\AIstock\backend\routers\paper_trading_v2.py` route enumeration (50 routes scanned via grep `@router\.|prefix`): prefix `/paper-v2`, no `/coldstart-sanity/*` or `/sentinel-*` route present.

No file in `F:\Dev\AIstock\backend\services\paper_trading_v2\` is named or references coldstart/sentinel.

### §6.2 Search results in codex branch (`qe-governance-integration-20260509`, HEAD c2352a9)

```
Grep coldstart-sanity|coldstart_sanity|sentinel-order|sentinel_order in F:/Dev/.../backend
-> 1 file: backend/tests/scripts/test_paper_v2_coldstart_sanity.py
```

The single match is the unit-test file (mocks `urlopen` via `monkeypatch`, no real router). `backend/routers/paper_trading_v2.py` in the codex branch is byte-identical in route surface to main (50 `@router` decorators, no coldstart-sanity or sentinel route). No file under `backend/services/paper_trading_v2/` mentions coldstart/sentinel.

### §6.3 Endpoint signature + handler

**Not present in either tree.** No FastAPI handler implementing POST `/paper-v2/coldstart-sanity/sentinel-order` exists in main HEAD `01dfb40`, codex `c2352a9`, baseline-post-r5 HEAD `94242c1`, or any commit between `2866f66..c2352a9`.

### §6.4 Verdict + severity

**CAVEAT (downgraded from BLOCKER), per task spec §"If it DOES exist..."/"If it exists in codex but NOT in main" guidance and Codex's own self-flagged caveat in the runbook.**

Rationale for CAVEAT rather than BLOCKER:
1. The task spec itself reads: "If it exists in codex branch but NOT in main (because R6 hasn't merged yet), that's EXPECTED — the gate is supposed to run post-merge on main."
2. Codex's commit message scope is "add ... sanity gate" — i.e., the GATE script, not the runtime endpoint. Adding the endpoint is a separate runtime concern.
3. The sanity gate is post-cutover tooling. The runbook §8.5 introductory paragraph (L607) is explicit: operators must run it ONLY after "backend `8001`, the Paper/R6 daemon, DB migrations, code sync, governance evidence backfill, and protected asset ledger backfill have already passed".
4. R6 merge brings additional work beyond `c2352a9`; the missing endpoint is presumed to be supplied by a sibling commit/PR in the R6 batch or by the runtime activation step (§9 of the runbook).

**However**, two outstanding risks for 9:30 tomorrow:
- (a) If R6 cutover does NOT add this endpoint before §8.5 is invoked, the gate will fail with HTTP 404 at Phase 2 sentinel_order_trigger -> verdict NO-GO -> 9:30 abort. The remedial-action mapping at L641 ("Verify the configured Paper v2 sentinel endpoint or daemon entry point.") confirms Codex anticipated this fallback.
- (b) The `--sentinel-endpoint` flag (L778) is configurable, suggesting operators can point at an alternate daemon entry point if the default isn't wired yet. Strategy should confirm with release commander which endpoint will be live at 9:29.

**Severity: HIGH but not BLOCKING for this verify**. Surface explicitly to strategy + release commander BEFORE 9:30 invocation.

## §7 Boundary confirmations

- `codex_code_modified` = **false** (read-only inspection of `qe-governance-integration-20260509` worktree only)
- `prod_db_touched` = **false** (no psycopg2 connect; dry-run skips DB phases by design)
- `dev_db_writes` = **false** (no INSERT/UPDATE/DELETE attempted; dry-run JSON shows `db_writes_executed=false`)
- `prod_8001_touched` = **false** (no HTTP request from this verify run; dry-run shows `prod_backend_http_touched=false`)
- Sanity gate invoked only with `--mode dry-run` (default); `--mode prod` NEVER invoked
- `codex_branch_merged` = **false** (verify doc written to baseline-post-r5 worktree on `claude/paper-v2-baseline-post-r5-20260511`, not into codex branch)
- Task A worktree (`branch-baseline-codex-qe`) untouched — separate worktree, separate branch, no overlap
- `frontend/tsconfig.tsbuildinfo` NOT staged (will verify via `rtk git status --short` before commit)
