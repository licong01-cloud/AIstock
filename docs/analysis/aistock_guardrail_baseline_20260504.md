# AIstock Guardrail Baseline Scan

- Generated at: 2026-05-04T04:45:42.076814+00:00
- Mode: `baseline_tracked`
- Files scanned: 1039
- Total findings: 563

## Summary By Severity

| Severity | Count |
|---|---:|
| P0 | 229 |
| P1 | 6 |
| P2 | 328 |
| P3 | 0 |

## Summary By Rule

| Rule | Count |
|---|---:|
| `ARCH-WSL-001` | 95 |
| `CONFIG-HARDCODE-001` | 6 |
| `ERR-FALLBACK-001` | 112 |
| `PROD-PORT-001` | 1 |
| `RESOURCE-TIMEOUT-001` | 14 |
| `TRADING-FALLBACK-001` | 21 |
| `UI-RAWJSON-001` | 314 |

## Interpretation

This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.
New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.
Historical findings should be triaged by module and burned down with regression tests.

## First 200 Findings

| Severity | Rule | File | Line | Remediation |
|---|---|---|---:|---|
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/data_source_manager_impl.py` | 316 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/data_source_manager_impl.py` | 521 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ERR-FALLBACK-001` | `backend/core/fund_flow_akshare_impl.py` | 40 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/qstock_news_data_impl.py` | 36 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/qstock_news_data_impl.py` | 182 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/stock_data_impl.py` | 110 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/stock_data_impl.py` | 118 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/stock_data_impl.py` | 126 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/stock_data_impl.py` | 261 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/stock_data_impl.py` | 271 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/stock_data_impl.py` | 988 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/unified_data_access_impl.py` | 1327 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/unified_data_access_impl.py` | 1409 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/unified_data_access_impl.py` | 1537 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/unified_data_access_impl.py` | 1554 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ERR-FALLBACK-001` | `backend/data_service/tdx_adapter.py` | 143 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/data_service/xtquant_adapter.py` | 345 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/data_service/xtquant_adapter.py` | 362 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/db/init_qe_archive_schema.py` | 133 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/db/init_qe_archive_schema.py` | 143 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/db/init_qe_archive_schema.py` | 176 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/db/init_quant_schema.py` | 694 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/init_quant_schema.py` | 712 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ARCH-WSL-001` | `backend/db/migrations/create_dispatch_tables.py` | 197 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `ARCH-WSL-001` | `backend/db/migrations/create_dispatch_tables.py` | 201 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 162 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 237 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 245 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 273 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/debug_logger.py` | 36 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/debug_logger.py` | 146 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/deepseek_client.py` | 1071 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 205 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 262 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 281 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 54 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 66 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 203 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 234 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 272 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 969 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 978 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 291 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 370 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 395 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 404 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 429 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 444 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 478 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 480 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 944 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/qmt_client.py` | 954 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ARCH-WSL-001` | `backend/infra/wsl_qlib_runner.py` | 13 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `ARCH-WSL-001` | `backend/infra/wsl_qlib_runner.py` | 97 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 159 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 256 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 286 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 438 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/qlib_exporter/router.py` | 212 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/qlib_exporter/router.py` | 1727 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/qlib_exporter/router.py` | 1936 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/qlib_exporter/router.py` | 2277 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/qlib_exporter/router.py` | 2744 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/quant_models/hmm/sector_hmm.py` | 103 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/monitor.py` | 88 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/monitor.py` | 98 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/qmt.py` | 88 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/qmt.py` | 609 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/qmt.py` | 668 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/routers/quantevolver.py` | 3792 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent.py` | 247 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent.py` | 293 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent.py` | 829 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent.py` | 875 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent.py` | 920 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent_llm_config.py` | 787 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/routers/rdagent_llm_config.py` | 37 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/routers/rdagent_llm_config_v2.py` | 28 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/rdagent_templates.py` | 127 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/strategies.py` | 219 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/strategies.py` | 251 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/strategies.py` | 285 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/routers/strategies.py` | 332 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/scripts/backfill_missing_factors.py` | 73 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ARCH-WSL-001` | `backend/scripts/backfill_model_training_from_logs.py` | 23 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/scripts/compare_xtquant_tdx_realtime.py` | 220 | Add timeout, cancellation, output capture, and process cleanup. |
| P1 | `CONFIG-HARDCODE-001` | `backend/scripts/diagnose_missing_factors.py` | 10 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/scripts/diagnose_missing_factors.py` | 81 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ARCH-WSL-001` | `backend/scripts/extract_factor_metrics_wsl.py` | 6 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P1 | `CONFIG-HARDCODE-001` | `backend/scripts/fix_missing_factor_code.py` | 21 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/scripts/link_bundles_by_workspace.py` | 23 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/scripts/restart_backend.py` | 8 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/scripts/restart_backend.py` | 23 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ERR-FALLBACK-001` | `backend/services/analysis_service.py` | 1615 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/analysis_service.py` | 1638 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/analysis_service.py` | 1655 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/factor_validator.py` | 140 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/paper_trading/training_service.py` | 353 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/services/paper_trading/training_service.py` | 277 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/blacklist_snapshot.py` | 38 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/callback_urls.py` | 15 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/completion_contract.py` | 145 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_analyst.py` | 239 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_analyst.py` | 1873 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_analyst.py` | 1903 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_analyst.py` | 1934 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_analyst.py` | 1626 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_metrics_scheduler.py` | 362 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_metrics_scheduler.py` | 354 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_rating_service.py` | 1268 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_rating_service.py` | 1277 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_rating_service.py` | 1340 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_rating_service.py` | 1348 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_rating_service.py` | 1518 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_rating_service.py` | 1540 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_transformation_service.py` | 49 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/factor_transformation_service.py` | 364 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/llm_client.py` | 16 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/multi_alpha_diagnostics.py` | 567 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/qe_feedback_service.py` | 104 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/qe_file_sync_client.py` | 156 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/qe_file_sync_client.py` | 77 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/services/quantevolver/qe_file_sync_client.py` | 59 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/services/quantevolver/qe_file_sync_client.py` | 177 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/qe_selection_service.py` | 433 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/qe_selection_service.py` | 473 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/qe_selection_service.py` | 475 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/quantevolver/qe_selection_service.py` | 477 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/templates/read_exp_res.py` | 132 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/templates/read_exp_res.py` | 389 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/quantevolver/templates/read_exp_res.py` | 1167 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_http_sync_service.py` | 219 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_results_api_client.py` | 29 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 117 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 128 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 138 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 349 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 391 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 393 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/rdagent_selection_service.py` | 395 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_task_sync_service.py` | 37 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_task_sync_service.py` | 870 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_task_sync_service.py` | 886 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/rdagent_task_sync_service.py` | 896 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/strategy_package/metrics_summary.py` | 34 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/services/strategy_package/models.py` | 173 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/services/tushare_sync_engine.py` | 289 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/watchlist_service.py` | 391 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/services/watchlist_service.py` | 419 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/strategies/ema_momentum_strategy.py` | 205 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/strategies/trend_following_strategy.py` | 194 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/strategies/twap_execution_strategy.py` | 197 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/strategies/volatility_breakout_strategy.py` | 218 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/tests/test_qe_archive_schema.py` | 70 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/tests/test_qe_archive_schema.py` | 98 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/analysis-trend/page.tsx` | 232 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/analysis-trend/page.tsx` | 252 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/analysis/page.tsx` | 330 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/analysis/page.tsx` | 390 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/analysis/page.tsx` | 406 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/analysis/page.tsx` | 2476 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/cloud-screening/page.tsx` | 157 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/cloud-screening/page.tsx` | 261 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/cloud-screening/page.tsx` | 288 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/page.tsx` | 164 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/components/AddModelDialog.tsx` | 95 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/components/EditModelDialog.tsx` | 156 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/components/EditModelDialog.tsx` | 211 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/components/EditModelDialog.tsx` | 233 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/components/StageMappingConfig.tsx` | 99 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 236 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 280 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 321 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 411 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 466 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 488 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/config/rdagent-llm/page.tsx` | 535 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/aistock-agents/page.tsx` | 142 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/aistock-agents/page.tsx` | 417 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/models/page.tsx` | 153 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/models/page.tsx` | 194 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/models/page.tsx` | 237 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/providers/page.tsx` | 146 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/providers/page.tsx` | 162 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/rdagent-config/page.tsx` | 154 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/rdagent-config/page.tsx` | 384 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/rdagent-config/page.tsx` | 383 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/llm/rdagent-config/page.tsx` | 388 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P0 | `PROD-PORT-001` | `frontend/src/app/local-data/page.tsx` | 362 | Use dev ports 8011/8012 and 3011/3012; production restart requires explicit user action. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 648 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 684 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 708 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 730 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 817 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 1412 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 1441 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 1462 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 1481 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 1518 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |
| P2 | `UI-RAWJSON-001` | `frontend/src/app/local-data/page.tsx` | 1882 | Show Chinese business labels, tables, cards, error states, and optional advanced JSON drawer only. |

Report truncated to 200 findings. See JSON output for full machine-readable details.
