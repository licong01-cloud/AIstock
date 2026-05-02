# QE Archive Phase 1 Schema/Repository Validation

Date: 2026-05-02

## Scope

- Implemented the first write-side foundation for the QE realtime experiment warehouse.
- Covered schema bootstrap, reproducibility config contract, repository write methods, and static guardrails.
- Did not hook realtime QE completion webhooks, artifact parsing workers, historical backfill jobs, or UI pages in this phase.

## Commands

```powershell
python -m compileall backend/db/init_qe_archive_schema.py backend/services/qe_archive
python -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q
rg -n "workspace_path|/mnt/f|\\\\wsl\\$|\\\\wsl\\.localhost|QE_WORKSPACE_WIN|RDAGENT_WORKSPACE_WIN" backend/db/init_qe_archive_schema.py backend/services/qe_archive
python backend/db/init_qe_archive_schema.py
```

## Results

- Compile: passed.
- Targeted pytest: `12 passed in 0.61s`.
- Workspace-path guardrail scan: no matches in the new QE archive DB/service files.
- DB bootstrap: `QE archive schema initialized: qe_archive_v1_20260502`.
- DB verification sample: `qe_archive_tables=27`, `schema_version=qe_archive_v1_20260502`.

## Business Oracles

- Every archived experiment/loop has a required `run_config` contract with canonical config, raw config, config hash, factor list/hash, config provenance, capture-complete flag, and missing config items.
- Reproducibility is explicit through `run_reproducibility_manifest`; incomplete records must be downgraded to `partial` or `audit_only`.
- Loop-card/detail metrics are structurally covered by scalar metrics, account summary, curves, symbol summaries, positions, orders, trades, raw payloads, and artifact manifests.
- Daily-frequency research-invalid runs remain filterable through `research_valid`, `invalid_reason`, `exclusion_tags`, and nullable `score_total`.

## Residual Risks / Next Phase

- Realtime webhook/outbox enqueue is not wired yet.
- Artifact download/parsing through node APIs is not implemented yet.
- Historical backfill is not implemented yet.
- Frontend charts and analytical aggregate views are not implemented yet.
- Production backend `8001` was not restarted.

## Asset Safety

- No QE/RD-Agent worker workspace files, model weights, StrategyPackage manifests, HMM snapshots, or runtime artifacts were modified.
