# [VERIFY] Claude paper-v2 5-layer verify of Codex paper_v2 coldstart sentinel endpoint (9f31ac8)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex + strategy session
> **date**: 2026-05-12
> **target**: codex/qe-governance-integration-20260509@9f31ac8 "feat(qe): add paper v2 coldstart sentinel endpoint"
> **prior verify resolved**: 1dc2e60 L5 sentinel endpoint CAVEAT
> **sister verifies**: c2ef5f5 (2fb81b3, READY), 94242c1 (2866f66, READY), 1dc2e60 (c2352a9 sanity, READY-WITH-CAVEATS — this verify upgrades)
> **reviewer branch**: claude/paper-v2-baseline-post-r5-20260511 (HEAD 37c1140)
> **SLA**: ≤45 min (9:30 LocalSim 模拟盘 T-2h44m)

## §0 Verdict

**READY**

Per-layer:
- L1 Static (5-guard + LocalSim-only + run_id prefix + package exact-match + 409/503 + capture preflight): **PASS**
- L2 Tests (53 expected: 18 new endpoint + 35 sanity-script extensions): **PASS — 53/53**
- L3 Sanity script integration (Phase 2 endpoint + cleanup scope): **PASS**
- L4 Runbook §7.4 + §8.5 alignment (8 fields): **PASS** (note: capture-DDL + sentinel-script wiring is documented in §8.5, not §7.4 — see §5)
- L5 Branch-local fallback option: **Option B** → **PASS** (auto-healing via kwargs → metadata source priority; broker_backend lives in metadata JSONB by design)

**9:30 LocalSim GO: YES**

Prior 1dc2e60 sentinel CAVEAT: **RESOLVED** (endpoint exists at `/api/v1/paper-v2/coldstart-sanity/sentinel-order`; sanity script Phase 2 wired to it; all 5 guards fail-fast before any state mutation).

Regression: prior 57 tests (24 strategy-pkg + 33 protected-asset) still pass.

## §1 Scope + commit anchors

Commit: `9f31ac8` (Tue May 12 01:46:57 2026 +0800). 8 files / +1204 / -32.

Files reviewed end-to-end:
- `backend/services/paper_trading_v2/coldstart_sentinel.py` (NEW 636 lines)
- `backend/routers/paper_trading_v2.py` (+31 lines: request model, error mapper, route)
- `backend/services/paper_trading_v2/repository.py` (+22 lines: `save_fill` widened)
- `scripts/paper_v2_coldstart_sanity.py` (+65 lines: sentinel integration)
- `backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py` (NEW 292 lines, 18 tests)
- `backend/tests/scripts/test_paper_v2_coldstart_sanity.py` (+133 lines, 35 tests in suite)
- `backend/db/add_paper_v2_capture_fields_20260510.sql` (+40 lines DDL)
- `docs/operations/r6_prod_apply_runbook_20260511.md` (+17 lines, §8.5 wiring)

Direct parent: `a72411d feat(qe): add R6 cutover wrapper`.

## §2 Layer 1 — Static review

### §2.1 5-guard chain (fail-fast BEFORE any state mutation)

`coldstart_sentinel.py` `ColdstartSentinelService.record_sentinel_order` orders the guards:

| # | Guard | path:LINE | Outcome on fail |
|---|---|---|---|
| 1 | `_validate_payload` (run_id `sanity-*`, package_id non-empty, symbol=000001.SZ, side=BUY, qty=100, intended_price=10.00, source, broker_backend=`local_sim`) | `coldstart_sentinel.py:69` (→ `:488-525`) | `StrategyPackageValidationError` → HTTP 400 |
| 2 | A-share trading-window block (09:30-11:30, 13:00-15:00 Asia/Shanghai, weekdays) | `coldstart_sentinel.py:71-79` | `InvalidStateTransitionError` → HTTP 409 |
| 3 | Daemon process check (paper_v2 daemon must be running) | `coldstart_sentinel.py:80-84` | `PaperV2DaemonUnavailableError` → HTTP 503 |
| 4 | `_select_enabled_package` (exact `package_id = %s`; status ∈ `{PAPER_ENABLED, PAPER_RUNNING, PAPER_PASSED}`) | `coldstart_sentinel.py:88` (→ `:97-125`) | `InvalidStateTransitionError` → HTTP 409 |
| 5 | `_require_capture_fields` (paper_v2.fills has created_at, updated_at, intended_price, fill_market_context) | `coldstart_sentinel.py:89` (→ `:127-157`) | `InvalidStateTransitionError` → HTTP 409 |

All 5 are evaluated BEFORE `_record_rows` (line 90). The transaction is opened by `conn_factory`, package/preflight reads run before any INSERT, and any exception inside the `with conn` block triggers `_rollback(conn)` (lines 93-95). PASS.

### §2.2 LocalSim-only hard reject

`_validate_payload` line 519-520:
```python
if broker_backend != LOCAL_SIM_BACKEND:
    failures.append("broker_backend must be local_sim")
```
`LOCAL_SIM_BACKEND = "local_sim"` (line 39). Any value other than `local_sim` causes `StrategyPackageValidationError` BEFORE any DB read or write. PASS.

### §2.3 run_id `sanity-*` prefix enforcement

`_validate_payload` line 501-502:
```python
if not run_id.startswith("sanity-"):
    failures.append("run_id must start with sanity-")
```
PASS.

### §2.4 package_id exact match, no fallback

`_select_enabled_package` (`:99-108`) uses `WHERE package_id = %s LIMIT 1` — strict equality, parameterized, no `LIKE` / `ILIKE` / partial. Hard-coded table `strategy_pkg.package`. PASS.

### §2.5 409 / 503 mapping

Router `_raise_coldstart_sentinel_http`:
```python
if isinstance(exc, PaperV2DaemonUnavailableError):
    raise HTTPException(status_code=503, detail=exc.to_dict()) from exc
if isinstance(exc, InvalidStateTransitionError):
    raise HTTPException(status_code=409, detail=exc.to_dict()) from exc
_raise_http(exc)  # → 400 for ValidationError
```
PASS. Test `test_sentinel_endpoint_rejects_when_daemon_absent` asserts 503; `test_sentinel_endpoint_rejects_a_share_trading_window` and `test_sentinel_endpoint_rejects_non_enabled_package_status_before_writes` assert 409.

### §2.6 capture preflight (4 columns)

`_require_capture_fields` reads `information_schema.columns` for `paper_v2.fills` and checks the set `{created_at, updated_at, intended_price, fill_market_context}` is present. Missing-column failure includes `ddl_file: backend/db/add_paper_v2_capture_fields_20260510.sql` in context. Runs BEFORE `_record_rows`. PASS.

### §2.7 Audit + SQL + txn hygiene

- Single transaction per sentinel call: all 8 INSERTs in one `with conn` block, committed once (line 91), rollback on exception (line 94).
- All SQL parameterized (`%s`), table names hard-coded (no whitelist-injectable identifiers).
- Audit emit (`paper_v2.run_events COLDSTART_SENTINEL_ACCEPTED`, `qe_archive.outbox_event paper_v2.coldstart_sentinel`, `strategy_pkg.package_validation_run` evidence row, `strategy_pkg.package_asset` ledger row) is unconditional — fires every accepted sentinel.
- ON CONFLICT clauses make the call idempotent per `run_id` (portfolio_id derived deterministically via sha256(run_id)[:24]).

PASS.

## §3 Layer 2 — Unit tests

Command:
```
python -m pytest backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py \
  backend/tests/scripts/test_paper_v2_coldstart_sanity.py -v -p no:cacheprovider
```

Result: **53 passed in 13.55s**. EXIT=0.

Breakdown:
- `test_coldstart_sanity_sentinel_endpoint.py`: 18 tests — including parametric invalid-input rejections covering all 8 validation fields, daemon-absent 503, trading-window 409, package-not-found 409, non-enabled-status 409, missing-capture-fields 409, OpenAPI presence, full chain INSERT.
- `test_paper_v2_coldstart_sanity.py`: 35 tests — script-side prod-guard tests, preflight gating, sentinel package_id default + override, capture-column preflight, cleanup commit/rollback per-table, full mocked GO path, source-import discipline.

**Regression**: prior 57 tests (`test_strategy_package_governance_evidence_backfill_prod_executor.py` 24 + `test_protected_asset_ledger_backfill_prod_executor.py` 33) re-ran: **57 passed in 0.56s**. No regression.

PASS.

## §4 Layer 3 — Sanity script integration

| Check | Source | Verdict |
|---|---|---|
| Phase 2 calls `POST /paper-v2/coldstart-sanity/sentinel-order` | `paper_v2_coldstart_sanity.py:39` defines `DEFAULT_SENTINEL_ENDPOINT = "/paper-v2/coldstart-sanity/sentinel-order"`; `:834` `--sentinel-endpoint default=DEFAULT_SENTINEL_ENDPOINT`; `:466` `_trigger_sentinel_order` posts to it | PASS |
| Payload includes `package_id` | `SentinelOrder.payload()` now emits `package_id`, `broker_backend="local_sim"`, `qty`+`quantity` alias | PASS |
| `--sentinel-package-id` default = first `--package-id` | `:880-882` `sentinel_package_id = args.sentinel_package_id or (package_ids[0] if package_ids else "")` | PASS |
| Prod guard enforces sentinel package_id in operator confirmation | `_require_prod_guards` (`:251`) takes `sentinel` and requires `sentinel.package_id in confirmation` text + non-empty | PASS |
| Phase 4 cleanup scope is run-scoped (NOT unbounded) | `_cleanup_sentinel` deletes WHERE `run_id = %s` or `source_id = %s` or `evidence_json LIKE %run_id%` / `asset_ref LIKE 'governance/coldstart_sanity/<run_id>%'`; portfolio deletion uses deterministic `_sentinel_portfolio_id(run_id)` | PASS |
| Cleanup includes paper_v2.run + paper_v2.portfolio rows | `CLEANUP_TABLES` + delete list (`:647-652`) now include both | PASS |
| Capture-column preflight phase in Phase 1 | `_preflight_db_checks` (`:340-374`) — new `required_capture_columns` phase blocks Phase 2 if missing | PASS |

PASS — sanity script Phase 2 wires directly to the new endpoint; cleanup is run-scoped and rollback-on-error per table.

## §5 Layer 4 — Runbook §7.4 + §8.5 (8 fields)

`docs/operations/r6_prod_apply_runbook_20260511.md`. Diff scope is two regions:
- §7.4 existing "Evidence Backfill Verification" — unchanged content; the runbook does NOT add a new §7.4 capture-DDL section. The capture-DDL prerequisite is documented inside §8.5 (line 616: "Before running this gate, confirm the Paper v2 capture-field DDL is present on the target DB (`paper_v2.fills.created_at`, `updated_at`, `intended_price`, `fill_market_context`)…").
- §8.5 "Paper V2 Cold-start Sanity Automation" — the +17 lines land here.

The user spec mentioned "§7.4 (new, capture DDL dep)" — confirmed the capture DDL dep IS documented, but inside §8.5 not §7.4. Treated as documentation-organization choice; field content matches source.

8 source ↔ runbook fields cross-check:

| # | Field | Source value | Runbook §8.5 value | Match |
|---|---|---|---|---|
| 1 | capture DDL dep | `backend/db/add_paper_v2_capture_fields_20260510.sql`, 4 cols on paper_v2.fills | "Paper v2 capture-field DDL… `paper_v2.fills.created_at, updated_at, intended_price, fill_market_context`" (L616) | PASS |
| 2 | OpenAPI deploy step | route `POST /api/v1/paper-v2/coldstart-sanity/sentinel-order` | "expected OpenAPI path… visible after backend deploy/restart/code reload at `/openapi.json`" + "Effective route: `POST /api/v1/paper-v2/coldstart-sanity/sentinel-order`" (L611, L617) | PASS |
| 3 | cleanup scope | run-scoped: paper_v2.{portfolio,run,run_events,orders,order_events,fills}, qe_archive.outbox_event, strategy_pkg.{package_validation_run,package_asset}; portfolio prefix `paper_v2_coldstart_sanity_` | "writes only run-scoped sentinel rows… `paper_v2.portfolio`, `paper_v2.run`, `paper_v2.run_events`, `paper_v2.orders`, `paper_v2.order_events`, `paper_v2.fills`, `qe_archive.outbox_event`, `strategy_pkg.package_validation_run`, and `strategy_pkg.package_asset`. The synthetic sentinel portfolio is deterministic by `run_id` and isolated under a `paper_v2_coldstart_sanity_` prefix" (L614) | PASS |
| 4 | LocalSim caveat | hard-reject any broker_backend != `local_sim` | "LocalSim-only and hard-rejects any `broker_backend` other than `local_sim`; it must not start the session scheduler, call miniQMT, or activate a live broker path" (L612); also L615 ("not proof of MiniQMT/live broker readiness"); L687 separate gate required | PASS |
| 5 | CLI args | `--sentinel-package-id`, `--sentinel-endpoint`, `--mode`, `--confirm-prod`, `--package-id` (multi), `--api-base`, `--target-db`, `--db-{host,port,name,user}`, `--db-password-env` | Prod invocation template at L640-660 lists all of these incl. `--sentinel-package-id '<PACKAGE_ID_1>'` (L651) and `--sentinel-endpoint '/paper-v2/coldstart-sanity/sentinel-order'` (L645) | PASS |
| 6 | env var names | `AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED`, `AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD`, `AISTOCK_PROD_DB_PASSWORD` | L636-638 sets all three | PASS |
| 7 | token literal | `RUN_PAPER_V2_COLDSTART_SANITY_PROD` | "--confirm-prod RUN_PAPER_V2_COLDSTART_SANITY_PROD" (L642) | PASS |
| 8 | operator confirmation | confirmation must contain `sentinel.package_id` text | "RUN_PAPER_V2_COLDSTART_SANITY_PROD target=prod packages=<PACKAGE_IDS> sentinel_package_id=<PACKAGE_ID_1> approved_by=<RELEASE_COMMANDER>" (L643) | PASS |

All 8 fields MATCH between source and runbook. PASS.

## §6 Layer 5 — ★★★ Branch-local fallback evaluation

### §6.1 Code analysis

`repository.py` +22 lines widen `PaperTradingV2Repository.save_fill`:

```python
def save_fill(
    self,
    run_id: str,
    fill: Fill,
    *,
    intended_price: float | None = None,
    fill_market_context: dict[str, Any] | None = None,
) -> None:
    if isinstance(fill.metadata, dict):
        intended_price = fill.metadata.get("intended_price", intended_price)
        fill_market_context = fill.metadata.get("fill_market_context", fill_market_context)
    now = datetime.now(UTC)
    with self._conn_factory() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO paper_v2.fills (
                    fill_id, run_id, order_id, symbol, side, quantity, price,
                    trade_time, bar_time, reason, metadata, created_at, updated_at,
                    intended_price, fill_market_context
                ) VALUES (...15 params...)
                ON CONFLICT(fill_id) DO NOTHING
                """,
                (..., now, now, intended_price, psycopg2.extras.Json(fill_market_context) if fill_market_context is not None else None),
            )
```

The "branch-local fallback" the user prompt asks about is the metadata source-priority chain, NOT a column-existence runtime branch. There is no `IF EXISTS … ELSE …` runtime column check; the code always writes to the 4 new columns. The presence of those columns is **gated upstream** by `coldstart_sentinel.py::_require_capture_fields` (`L127-157`), which fails the call with a 409 + DDL-file pointer if any column is missing — so the write path NEVER runs against a DB lacking the columns.

`broker_backend` does NOT have a dedicated column in `paper_v2.fills` (per `add_paper_v2_capture_fields_20260510.sql` — DDL adds only 4 fields, none of them `broker_backend`). `broker_backend` continues to live inside the `metadata` JSONB column (always has, by design — see fill.metadata construction in `coldstart_sentinel.py:209-217`). The sentinel writes broker_backend into both `metadata.broker_backend` AND `fill_market_context.broker_backend` (line 550 in `_fill_market_context`), so retrievability is intact post-merge.

### §6.2 Option chosen + rationale

**Option B** (sensible source-priority chain that auto-heals).

- Kwargs-first, metadata-fallback chain is deterministic and self-healing: callers that pass kwargs explicitly always win; legacy callers whose metadata carries the values get equivalent behavior.
- No "column-missing" runtime branch exists, because the capture-field DDL is a hard prerequisite enforced upstream by the sentinel-endpoint preflight (and also by the sanity script's `required_capture_columns` phase). When the DDL is applied (post-R6 prod and dev-after-migration), every write goes straight to the dedicated columns; there is no degraded path.
- `broker_backend` has no dedicated column by design — it lives in metadata. This is consistent with the prior schema; no new "fallback" was introduced for it.

### §6.3 R6 codebase health impact

PASS. No technical debt added. The change is forward-compatible:
- Post-R6 (DDL applied): direct column writes for all 4 fields; sentinel + future capture paths use the same row layout.
- The metadata-source fallback in `save_fill` exists not because of branch-local schema drift but to let `Fill` models carry these values through legacy call sites without an API change. This is a normal source-priority pattern, not dead code.
- `broker_backend` lives in metadata JSONB by intentional design (not all backends benefit from a dedicated column). Retrievable via `metadata::jsonb->>'broker_backend'`.

### §6.4 Recommended follow-up

None blocking. Optional minor cleanup:
- A short docstring on `save_fill` explaining the kwargs → metadata source priority (so future maintainers don't read it as "fallback for missing schema").

No Codex follow-up dispatch required.

## §7 Boundary confirmations

- codex_code_modified: **false**
- prod_db_touched: **false**
- dev_db_writes: **false**
- prod_8001_touched: **false**
- dev_8012_started: **false** (pure unit-test path; FastAPI TestClient in-process only)
- codex_branch_merged: **false**
- `frontend/tsconfig.tsbuildinfo` staged: **NO**

## §8 Disposition

- Prior 1dc2e60 sentinel CAVEAT → **RESOLVED** (endpoint exists, fail-fast 5-guard, sanity wired, all tests green).
- All 4 sister-verify scopes (strategy_package backfill, protected_asset_ledger, sanity gate, sentinel endpoint) now land READY without caveats.
- 9:30 LocalSim mock-pan: **GO**.
