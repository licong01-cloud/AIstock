# QE Warehouse MCP

Route warehouse, archive, outbox, ingestion, backfill and missing-ingestion requests to `aistock-qe-archive`. Treat data repair or backfill execution as confirmed actions: plan first, then require explicit confirmation and approval. Do not route warehouse language to local-data unless the user explicitly asks about local dataset sync health.
