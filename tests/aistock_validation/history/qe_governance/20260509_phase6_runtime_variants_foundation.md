# QE Governance Phase 6 Runtime Variants Foundation - 2026-05-09

Task/branch: `codex/qe-phase-6-runtime-variants-20260509`

Scope: Adds the StrategyPackage runtime variant foundation. Runtime variants are stored separately from frozen manifests, carry an independent variant hash, preserve a locked core hash, and cannot become Paper candidates before validation passes.

## Safety

- Production 8001 touched: no.
- Production DB written: no.
- Dev DB written: no; validation used unit/static tests only.
- Protected assets touched: no StrategyPackage frozen manifest, model weight, HMM snapshot, QE/RD-Agent artifact, Paper ledger, or validated policy modified.
- `AGENTS.md` modified: no.
- `main` merged: no.
- DDL scope: additive `strategy_pkg.package_runtime_variant` table only, with PostgreSQL comments on table and every column.

## Validation Results

| Gate | Evidence | Status |
| --- | --- | --- |
| Compile runtime variant code | `python -m py_compile backend/services/strategy_package/runtime_variant.py backend/services/strategy_package/repository.py backend/services/strategy_package/service.py backend/routers/strategy_packages.py backend/tests/strategy_package/test_runtime_variants.py` | Pass |
| Runtime variant contract tests | `pytest backend/tests/strategy_package/test_runtime_variants.py -q -p no:cacheprovider` | Pass: `8 passed` |
| StrategyPackage module tests | `pytest backend/tests/strategy_package -q -p no:cacheprovider` | Pass: `65 passed` |
| Governance integration subset | `pytest backend/tests -q -p no:cacheprovider -k "runtime_variant or strategy_package or seed_contract or promotion_review"` | Pass: `72 passed, 898 deselected` |
| Guardrail changed-files scan | `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` | Pass: `findings=0, blocking=0` |
| Whitespace | `git diff --check` | Pass; CRLF warnings only for existing line-ending behavior |

## Business Validation

- Runtime variant configs reject frozen-core keys such as `model_asset`, `factor_set`, `alpha_components`, and `manifest_sha256`.
- Different runtime configs produce different `variant_hash` values while preserving the same `locked_core_hash`.
- `paper_candidate=true` requires `VALIDATION_PASSED` plus validation evidence.
- Repository-level checks reject mismatched `manifest_sha256` or `locked_core_hash`, even if callers bypass service construction.

## Residual Risks

- Live dev DB migration was not executed in this slice.
- Paper v2 does not yet consume runtime variants; a later phase must wire validated variants into Selection/Paper using dev-port integration tests before any `main` merge.
