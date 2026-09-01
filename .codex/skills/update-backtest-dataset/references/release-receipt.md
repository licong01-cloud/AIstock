# Release receipt and signoff

## Contents

- [Cataloged evidence](#cataloged-evidence)
- [Terminal outcomes](#terminal-outcomes)
- [Required validation](#required-validation)
- [Receipt interpretation](#receipt-interpretation)
- [Error taxonomy](#error-taxonomy)
- [Production boundary](#production-boundary)

## Cataloged evidence

A successful candidate release is visible as one atomic catalog projection containing:

- submission, logical request, resolved intent, run generation and attempt identities;
- release ID/digest, candidate identity/path identity and committed marker;
- semantic/source/PIT/artifact/validation/resource fingerprints;
- per-component/partition action plan and lineage;
- artifact manifest, hashes, schemas, rows/ranges and CAS refs;
- current-source attestation bound to the exact release;
- validation/signoff and resource/performance receipts;
- `metadata/index_context_manifest.json` identity/readback plus moneyflow raw/derived parity evidence;
- terminal run outcome and submission/run events.
- for the first PIT v2 migration: exact `plan_id/plan_digest/fixed_cutoff/scope`, frozen sample/event/index windows and
  the validation-only versus full PIT binding.

Candidate bytes without a cataloged committed marker and matching terminal transaction are not a published release. A receipt path printed by a child is not enough.

Attestation is independent evidence under control CAS/catalog. Re-attestation never modifies the target candidate. A build publishes candidate bytes and its current-source attestation together; an expired attestation freshness TTL cannot be reused as a new no-op without a fresh probe.

## Terminal outcomes

Interpret run-level outcome separately from component actions:

| Outcome | Meaning |
|---|---|
| `NO_OP_VERIFIED` | fresh source/PIT/validator evidence proves an existing release remains current |
| `REATTESTED` | a new independent attestation was created; candidate bytes unchanged |
| `CANDIDATE_VALIDATED` | at least one component was materialized and the candidate passed signoff; inspect mixed actions |
| `CANCELLED` | durable cancel reached a safe checkpoint before publish commit |
| `FAILED_*` / `BLOCKED_*` | no success; inspect typed code/context ref |

Do not label `REUSE`, `INCREMENTAL` or `SELECTIVE_REBUILD` as the whole run unless every component action actually matches.

## Required validation

Full `qe_hmm_full_v2` canonical candidate signoff requires all applicable checks PASS; no WARN/SKIP substitutes for a required component. `qe_hmm_full_v1` retains the same checks only for historical reproduction/re-attestation:

1. artifact inventory, hashes, schema/dtype/index ordering and date/cutoff coverage;
2. frozen PIT snapshot/digest, multi-span application and zero out-of-span stock rows;
3. daily and minute Qlib value-level parity, QFQ basis, limit/suspend/pre-close references, strictly ordered segmented
   canonical CSV authority and consumer smoke;
4. minute DB/overlay missing-key union exactly matches canonical keys; provider overlap mismatch count zero; source and
   dump-update code batches never exceed the active pressure rung/hard maximum 20;
5. `tushare_moneyflow_shares_yuan_v1`, H5/static raw-field parity, 9 volume ×100 and 9 amount ×10000 source conversions;
6. `mf_total_net_amt == mf_net_amt`, `mf_total_net_vol == mf_net_vol`, correct amount/volume denominators and 5/20 rolling derivation;
7. static 121 data columns, `l2_code_id int16`, unknown `-1`, sector interval parity;
8. exact 12-index list/roles/start/weight mapping, three-column `index.txt`, DB/provider parity, H5/bin units,
   candidate-local index manifest readback, stock/index instrument isolation and HMM benchmark unchanged;
9. QE daily/minute and HMM producer fixture/smoke bound to this release; HMM consumer activation remains not activated;
10. resource hard-cap/readback receipt, bounded query/read/log behavior and comparable performance evidence required for source merge.

Shape, last date, row count, file size or sampled values alone cannot sign off a release.

## Receipt interpretation

Read the summary first, then follow cataloged refs. Do not print full logs or full JSON to an interactive terminal.

Required summary fields:

```text
schema/capability versions
profile + requested/effective cutoff + scope
submission/logical/resolved/run/attempt/release/attestation IDs
run outcome + component actions/reasons
candidate/marker/artifact/source/PIT/validation digests
validation totals and required failures
resource peaks + wait/compute/provider time + row/query/byte totals
production/node1/DB/runtime/cleanup gates
```

Verify CAS refs and committed marker through the catalog reader; never accept a user-supplied arbitrary path. Log/event pagination must be bounded and cursor-bound to endpoint/principal/run/filter/sort. A log continuation is the returned
`(next_log_id,next_generation,next_byte_offset)` position; it must not be replaced with a fresh tail read.

Receipt/logs must contain credential locations only. Never include token, password, private key, DSN secret or operator authorization value.

## Error taxonomy

| Category | Examples | Handling |
|---|---|---|
| retryable | transient DB disconnect, temporary file lock, provider 5xx | bounded retry/backoff in same policy |
| resource telemetry | host/WSL memory, commit headroom, paging, predicted free space | receipt/warning only; never wait, pause, retry or terminalize |
| operating-system failure | child OOM/termination, actual ENOSPC, Docker/WSL failure | fail the current attempt with the real error; no automatic retry |
| orphan hold | expired owner tree alive/unknown | retain both leases until quiescent |
| source blocked | watermark/partition/PIT not ready | wait/block; no fill or scope reduction |
| terminal contract | schema/PIT/moneyflow/index/bin-H5 conflict | fail fast |
| provider terminal | 40203, overlap conflict, incomplete/invalid 240 bars | fail fast; report pending keys |
| identity conflict | idempotency/fingerprint/manifest/fence mismatch | fail fast; do not edit state |
| cancelled | durable request | checkpoint and exit if before commit point |

Never convert a terminal error into retry success by switching unapproved authority, forward-filling, zero-filling, dropping stocks/dates/fields/components or running a legacy exporter.

## Production boundary

Candidate signoff does not imply any of these actions:

```text
production_activation
production pointer/symlink migration
node1 distribution
DB repair or DDL/DML
backend/Worker/scheduler restart or registration
dependency/client installation
cache cleanup or candidate deletion
```

Report each as `not_requested`, `not_authorized`, `pending`, `complete` or its typed failure. Only a separate target-specific authorization may change it. Source merge itself does not grant runtime activation.

If validation used only temp/fake fixtures, report:

```text
source_state=source_ready_fixture_verified
mixed_daily_minute_factor_direct_e2e=fixture_verified
selective_override_clean_full_equivalence=fixture_verified
platform_hard_cap_evidence=fixture_platform_verified_real_full_pending
runtime_real_data_evidence=not_run_not_authorized
real_full_scale_performance=pending
production_activation=not_requested
```

Do not claim a real monthly candidate, performance result or production-ready dataset without its real terminal receipt.
