# BUG-511 Repo Hygiene P4 Factor/QE Artifact Cleanup Manifest

- date: 2026-06-24
- source_bug: `BUG-510`
- source_manifest: `tests/aistock_validation/history/repo_hygiene/BUG-510_p4_cleanup_manifest_20260624.md`
- github_issue: https://github.com/licong01-cloud/AIstock/issues/1584
- policy: user confirmed January/February 2026 P4 generated artifacts can be deleted directly; no Nightly automatic deletion.

## Deleted User-Confirmed P4 Artifacts

- `factors_ui/instrument_format_analysis.json` - generated analysis JSON; BUG-510 evidence showed modified `2026-01-31 18:32:19`; repo hygiene audit reported it as unreferenced generated artifact.
- `git_ignore_folder/factor_implementation_source_data_test/test_static_factors.parquet` - generated factor test dataset; BUG-510 evidence showed modified `2026-02-10 14:23:48`, size 28,249,065 bytes.
- `git_ignore_folder/factor_implementation_source_data_test/test_static_factors_schema.json` - generated schema sidecar; BUG-510 evidence showed modified `2026-02-10 14:23:48`.
- `git_ignore_folder/factor_implementation_source_data_test/test_validation_report.json` - generated validation report sidecar; BUG-510 evidence showed modified `2026-02-10 14:24:19`.

## Safety Checks

- These files were previously isolated by BUG-510 as P4 delete candidates or deferred P4 domain artifacts.
- User explicitly approved direct cleanup for January/February 2026 files.
- Deletion is performed in an isolated worktree/branch and persisted through PR/CI.
- Historical references in BUG-510 records and manifests are evidence-only references, not runtime or source references.

## Production Gates

- production_ddl_gate: `noop`
- production_frontend_dependency_gate: `noop`
- production_backend_dependency_gate: `noop`

