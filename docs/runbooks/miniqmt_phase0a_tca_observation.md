# MiniQMT Phase 0A TCA Observation Runbook

## Scope and safety boundary

This runbook applies only to MiniQMT SIM `event_loop` observation. It creates
or reads immutable TCA evidence; it does not submit, cancel, reprice, tick, or
reconcile broker orders, and it does not grant any LIVE capability.

The EOD hook is default-off. A failed observation must remain an observation
failure: it must not change a simulation run status, broker call count, or the
result of the already-completed reconciliation.

## 1. Code merged is not activation

Code merged means only that the read repository, REST adapters, export CLI, and
default-off EOD hook exist in `main`. It does not mean that configuration is
persisted, a service has loaded it, an observation rebuild has run, or a
prospective receipt exists.

Before treating this capability as available, separately record all of:

- code merge commit and test/CI receipt;
- persisted configuration state;
- loaded runtime configuration state after a user-performed restart, if one is
  required;
- EOD hook activation state;
- real SIM prospective receipt/evidence state.

## 2. Configuration persistence

Never place the value of an HMAC key, broker credential, or account ID in a
ticket, log, export, or this runbook. Persist the following names through the
approved runtime configuration channel:

| Variable | Required when | Purpose |
|---|---|---|
| `MINIQMT_TCA_ACTIVE_READ_VERSION` | parent/list/default TCA reads and export | Complete version tuple plus `config_sha256`; no implicit latest-code fallback. |
| `AISTOCK_TCA_EXPORT_HMAC_KEY` | all parent/list/TCA APIs and export | Non-reversible account pseudonym HMAC key. |
| `AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION` | with HMAC key | Version written to API responses and export manifest. |
| `MINIQMT_TCA_EOD_OBSERVATION_ENABLED=false` | normal/default operation | Keeps the EOD rebuild hook disabled. |
| `MINIQMT_TCA_EOD_CODE_COMMIT` | only when explicitly enabling EOD observation | Immutable audit identity for a rebuild receipt. |
| `MINIQMT_TCA_EOD_OPERATOR_PSEUDONYM` | only when explicitly enabling EOD observation | Non-secret operator pseudonym for receipt audit. |

If the active tuple or either HMAC identity component is missing, read/export
operations fail loudly with a `503` reason code. Do not add a temporary,
fixed, reversible, or raw-account fallback.

## 3. Activation requires explicit authorization

The user alone performs service restarts. Do not restart backend, frontend,
TDX, MiniQMT, or any scheduler process from this workflow.

Before enabling `MINIQMT_TCA_EOD_OBSERVATION_ENABLED=true`, obtain explicit
authorization and verify all of the following in read-only mode:

1. The committed code is in `main`; production DDL status for this batch is
   `noop` unless a separately committed migration is introduced.
2. The target scope is MiniQMT SIM, not LIVE.
3. A post-close terminal run has a completed reconciliation with
   `reconcile_after_submit.run.status=SUCCEEDED`.
4. HMAC key/version, active read version, code commit, and operator pseudonym
   are persisted without disclosing secret values.
5. The planned observation is outside the broker critical path and no B0
   submit/cancel/query behavior will be changed.

After the user performs any required restart, verify the loaded configuration
through the read-only API/diagnostics. Activation is not inferred merely from a
process being alive.

## 4. Evidence collection

Use the three read-only endpoints for parent/list/TCA evidence. The list cursor
is signed and filter-bound; a malformed, stale-schema, or cross-filter cursor
must return a loud `400`, never silently restart from page one.

For a canonical evidence artifact, use the CLI with a user-selected output
path:

```powershell
python scripts/export_miniqmt_execution_tca_evidence.py `
  --binding-id <binding-id> `
  --trade-date <YYYY-MM-DD> `
  --output <approved-output-path> `
  --format ndjson
```

The CLI refuses to overwrite an existing artifact unless `--overwrite` is
explicit. Its manifest must contain schema/version/query/hash and HMAC
key-version metadata, while its records must contain no raw account, secret,
broker credential, or raw transport payload.

A real prospective SIM receipt is required for Phase 0A exit evidence. Local
mock output, an API smoke, or a disabled-hook result is not a prospective
receipt and must not be represented as one.

The manual rebuild command is SIM-only and dry-run by default. It prints only
account pseudonyms. `--execute` performs an immutable evidence materialization
and therefore requires separate user authorization; Codex must not run it
against a production database without that authorization.

```powershell
python scripts/rebuild_miniqmt_execution_tca.py `
  --binding-id <binding-id> `
  --trade-date <YYYY-MM-DD> `
  --account-id <internal-sim-account> `
  --as-of <ISO-timestamp-with-offset> `
  --code-commit <commit> `
  --operator-pseudonym <pseudonym>
```

## 5. Observation alerts and response

Metrics and alerts are facts, not execution gates. The expected metric is
`miniqmt_tca_observation`; its payload includes `status`, `reason_code`,
`stage`, scope identifiers, and `execution_gate=false`.

For `MINIQMT_TCA_OBSERVATION_FAILURE` or any
`ADAPTIVE_IS_TCA_*` EOD failure:

1. Preserve the original scheduler result, run status, reconciliation outcome,
   and broker evidence.
2. Capture the reason code, stage, run/binding identifiers, and emitted metric
   receipt; do not paste secret values.
3. Keep the hook disabled or disable it if it was enabled.
4. Diagnose/rebuild manually in SIM only after authorization; never compensate
   by placing or cancelling orders.

## 6. Rollback and Monday B0 verification

Rollback is a writer stop: set
`MINIQMT_TCA_EOD_OBSERVATION_ENABLED=false` and let the user perform any
required restart. Retain immutable evidence tables and existing receipts; do
not delete TCA rows as a rollback action.

Monday's B0 SIM verification is separate from TCA activation. It validates the
already-merged BUG-599/600/604/614/615/617 execution behavior: scheduler
progress, Postgres row writes, broker side effects, pending tick driving,
marketable-limit/tail-sweep behavior, and visible orders. It must not wait for
the default-off TCA hook or be reported as Phase 0A completion by itself.
