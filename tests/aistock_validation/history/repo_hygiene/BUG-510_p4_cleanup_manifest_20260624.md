# BUG-510 Repo Hygiene P4 Cleanup Manifest

- date: 2026-06-24
- source_audit: `tmp/validation/code-intelligence/manual-repo-hygiene-final-20260624/repo-hygiene-orphan-audit.json`
- policy: cleanup is human-reviewed; no automatic Nightly deletion.

## Deleted Low-Risk Runtime/Dependency Residue

- `backend/requirements.torch.txt` — isolated, unreferenced Torch pin; current dependency management does not use it.
- `log/2026-02-05_15-09-16-645141/LITELLM_SETTINGS/52996/2026-02-05_15-09-32-957955.pkl` — tracked runtime LiteLLM pickle log; no references.
- `log/2026-02-05_15-12-21-893866/LITELLM_SETTINGS/52712/2026-02-05_15-12-34-026809.pkl` — tracked runtime LiteLLM pickle log; no references.
- `monitoring/textfile_collector/task_verify.txt` — one-off textfile collector snapshot; collector scripts remain tracked.

## Archived Instead Of Deleted

- `scripts/dev_db/phase2_pre_apply_baseline_20260510.txt` -> `tests/aistock_validation/history/repo_hygiene/BUG-510_phase2_pre_apply_baseline_20260510.txt`
- `scripts/dev_db/phase2_post_apply_validation_20260510.txt` -> `tests/aistock_validation/history/repo_hygiene/BUG-510_phase2_post_apply_validation_20260510.txt`

These two files are dev DB apply evidence and should remain as validation history, not executable script-directory clutter.

## Deferred Domain-Specific Review

- `git_ignore_folder/factor_implementation_source_data_test/test_static_factors.parquet` — modified `2026-02-10 14:23:48`, size 28,249,065 bytes.
- `git_ignore_folder/factor_implementation_source_data_test/test_static_factors_schema.json` — modified `2026-02-10 14:23:48`, paired with the parquet dataset.
- `git_ignore_folder/factor_implementation_source_data_test/test_validation_report.json` — modified `2026-02-10 14:24:19`, paired with the parquet dataset.
- `factors_ui/instrument_format_analysis.json` — modified `2026-01-31 18:32:19`, paired with legacy `factors_ui/factor_*.py` files.

These are not deleted in BUG-510 because they need factor/QE domain review as whole asset groups.

## Production Gates

- production_ddl_gate: `noop`
- production_frontend_dependency_gate: `noop`
- production_backend_dependency_gate: `noop`
