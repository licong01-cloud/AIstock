# AIstock Guardrail Baseline Scan

- Generated at: 2026-05-04T06:04:07.251977+00:00
- Mode: `baseline_tracked`
- Files scanned: 1039
- Total findings: 1632

## Summary By Severity

| Severity | Count |
|---|---:|
| P0 | 341 |
| P1 | 109 |
| P2 | 1182 |
| P3 | 0 |

## Summary By Rule

| Rule | Count |
|---|---:|
| `ALGO-COMPLEXITY-001` | 852 |
| `ARCH-WSL-001` | 128 |
| `CONFIG-HARDCODE-001` | 62 |
| `DEBUG-FAILFAST-001` | 3 |
| `ERR-FALLBACK-001` | 187 |
| `MEMORY-DATAFRAME-001` | 32 |
| `PROD-PORT-001` | 1 |
| `RESOURCE-TIMEOUT-001` | 16 |
| `SCRIPT-LOCATION-001` | 12 |
| `TRADING-FALLBACK-001` | 25 |
| `UI-RAWJSON-001` | 314 |

## Interpretation

This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.
New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.
Historical findings should be triaged by module and burned down with regression tests.

## First 200 Findings

| Severity | Rule | File | Line | Remediation |
|---|---|---|---:|---|
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/ai_agents_impl.py` | 118 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/ai_agents_impl.py` | 734 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/ai_agents_impl.py` | 737 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/ai_agents_impl.py` | 821 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 222 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 764 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 974 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 1177 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 1381 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 1584 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/agents/trend_analysis.py` | 1673 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ARCH-WSL-001` | `backend/config_manager_compat.py` | 75 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `TRADING-FALLBACK-001` | `backend/config_manager_compat.py` | 304 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P1 | `CONFIG-HARDCODE-001` | `backend/config_manager_compat.py` | 353 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/config_manager_compat.py` | 362 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/config_manager_compat.py` | 411 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/config_manager_compat.py` | 438 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/config_manager_compat.py` | 469 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P1 | `CONFIG-HARDCODE-001` | `backend/config_manager_compat.py` | 475 | Move paths/secrets to configuration, environment, DB catalog, or manifest; never commit secrets. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/config_manager_compat.py` | 489 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/data_source_manager_impl.py` | 316 | Add timeout, cancellation, output capture, and process cleanup. |
| P2 | `RESOURCE-TIMEOUT-001` | `backend/core/data_source_manager_impl.py` | 521 | Add timeout, cancellation, output capture, and process cleanup. |
| P0 | `ERR-FALLBACK-001` | `backend/core/fund_flow_akshare_impl.py` | 40 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/fund_flow_akshare_impl.py` | 235 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/fund_flow_akshare_impl.py` | 431 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/market_sentiment_data_impl.py` | 1015 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/market_sentiment_data_impl.py` | 1110 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/market_sentiment_data_impl.py` | 1184 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/core/qstock_news_data_impl.py` | 36 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/core/qstock_news_data_impl.py` | 182 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/qstock_news_data_impl.py` | 307 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/quarterly_report_data_impl.py` | 494 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/risk_data_fetcher_impl.py` | 362 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/risk_data_fetcher_impl.py` | 163 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/risk_data_fetcher_impl.py` | 228 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/risk_data_fetcher_impl.py` | 309 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/risk_data_fetcher_impl.py` | 426 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/risk_data_fetcher_impl.py` | 518 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
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
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/unified_data_access_impl.py` | 1248 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/unified_data_access_impl.py` | 1263 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/core/unified_data_access_impl.py` | 2291 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/api.py` | 354 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/api.py` | 302 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 351 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 60 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 298 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 425 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 469 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 518 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 564 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 606 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 607 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 647 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 938 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 977 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/qe_data_service.py` | 981 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/realtime_factor_data_loader.py` | 336 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/realtime_factor_data_loader.py` | 282 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/realtime_factor_data_loader.py` | 466 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/data_service/tdx_adapter.py` | 143 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/data_service/xtquant_adapter.py` | 345 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/data_service/xtquant_adapter.py` | 362 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/data_service/xtquant_adapter.py` | 371 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `TRADING-FALLBACK-001` | `backend/db/init_qe_archive_schema.py` | 133 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/db/init_qe_archive_schema.py` | 143 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `TRADING-FALLBACK-001` | `backend/db/init_qe_archive_schema.py` | 176 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P0 | `ERR-FALLBACK-001` | `backend/db/init_quant_schema.py` | 694 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/init_quant_schema.py` | 712 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/add_catalog_id_relations.py` | 21 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/add_holding_period_class.py` | 17 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/add_multi_period_ic_columns.py` | 14 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ARCH-WSL-001` | `backend/db/migrations/create_dispatch_tables.py` | 197 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `ARCH-WSL-001` | `backend/db/migrations/create_dispatch_tables.py` | 201 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/create_factor_correlations_table.py` | 10 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/fix_factor_code_quality.py` | 19 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/fix_factor_code_quality.py` | 24 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/fix_factor_code_quality.py` | 104 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/fix_factor_code_quality.py` | 194 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/fix_factor_code_quality.py` | 212 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/fix_factor_code_quality.py` | 219 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/rebuild_factor_correlations.py` | 167 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/migrations/run_watchlist_migration.py` | 37 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 162 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 237 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 245 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/db/pg_pool.py` | 273 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/pg_pool.py` | 57 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/pg_pool.py` | 66 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/pg_pool.py` | 210 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/pg_pool.py` | 254 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/db/seed_quant_universe_and_static.py` | 238 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/inference_engine.py` | 252 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `TRADING-FALLBACK-001` | `backend/inference_engine.py` | 99 | Make downgrade explicit, audited, tested, and UI-visible; otherwise fail fast. |
| P1 | `MEMORY-DATAFRAME-001` | `backend/inference_engine.py` | 200 | For large data paths, add chunking, date/symbol/column bounds, row-count evidence, or justify why the input is small. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/debug_logger.py` | 36 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/debug_logger.py` | 146 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/debug_logger.py` | 46 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/debug_logger.py` | 100 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/debug_logger.py` | 101 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/debug_logger.py` | 102 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/deepseek_client.py` | 1071 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 340 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 613 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 620 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 772 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 783 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 885 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 887 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 902 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 1027 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 1034 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/deepseek_client.py` | 1094 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 205 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 262 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 281 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 54 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 66 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 203 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/infra/network_optimizer.py` | 234 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/network_optimizer.py` | 18 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
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
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/qmt_client.py` | 58 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ARCH-WSL-001` | `backend/infra/wsl_qlib_runner.py` | 13 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P0 | `ARCH-WSL-001` | `backend/infra/wsl_qlib_runner.py` | 97 | Use worker API or AIstock-owned artifact store with manifest/hash; do not use filesystem shortcuts. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/wsl_qlib_runner.py` | 111 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/wsl_qlib_runner.py` | 113 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/infra/wsl_qlib_runner.py` | 175 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 159 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 256 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 286 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/ingestion/news_ingestion.py` | 438 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 307 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 309 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 342 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 358 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1200 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1309 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1356 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1393 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1412 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1464 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1491 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1533 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1544 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1791 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1802 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1809 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 1814 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 3053 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 3084 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/ingestion/tdx_scheduler.py` | 3136 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 91 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 74 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 79 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 112 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 117 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 127 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 129 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 151 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 172 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 203 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 222 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 232 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 236 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 374 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 378 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 383 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 388 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 393 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 398 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 403 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 408 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 414 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P0 | `ERR-FALLBACK-001` | `backend/main.py` | 418 | Fail fast with structured status/error context; never hide business failure behind an empty/default success. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/main.py` | 110 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/qlib_exporter/authoritative_bin_exporter.py` | 819 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/qlib_exporter/authoritative_bin_exporter.py` | 820 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |
| P2 | `ALGO-COMPLEXITY-001` | `backend/qlib_exporter/authoritative_bin_exporter.py` | 886 | Document row-count bounds, join keys, row-explosion risk, and batching/vectorization strategy for large quant workloads. |

Report truncated to 200 findings. See JSON output for full machine-readable details.
