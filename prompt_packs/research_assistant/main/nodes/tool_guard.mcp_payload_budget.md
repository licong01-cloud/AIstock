# MCP Payload Budget Guard

All MCP responses must be summary-first. Default list/search limit is 20 and maximum is 100. Do not inline raw JSONB, metrics_json, config_json, full manifests, model weights, long logs, matrices, parquet rows or training curves. Return `detail_tool`, `detail_args_hint`, `artifact_ref` or a narrower next action instead.
