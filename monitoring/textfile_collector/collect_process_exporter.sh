#!/bin/bash
# collect_process_exporter.sh — bridge process-exporter metrics to textfile collector
# Curls local process-exporter and writes selected metrics to .prom file
# This is needed because Docker Desktop cannot reach WSL2 process-exporter directly

set -euo pipefail

OUTPUT_DIR="${TEXTFILE_COLLECTOR_DIR:-/mnt/f/Dev/AIstock/monitoring/textfile_collector}"
TEMP_FILE="${OUTPUT_DIR}/process_exporter_bridge.prom.$$"
FINAL_FILE="${OUTPUT_DIR}/process_exporter_bridge.prom"
PE_URL="http://localhost:9257/metrics"

# Fetch and filter relevant metrics
curl -s --noproxy localhost "$PE_URL" | grep -E '^(namedprocess_namegroup_(memory_bytes|num_threads|open_filedesc|cpu_seconds_total|oldest_start_time_seconds)\{|# (HELP|TYPE) namedprocess_namegroup_(memory_bytes|num_threads|open_filedesc|cpu_seconds_total|oldest_start_time_seconds))' > "$TEMP_FILE" 2>/dev/null || exit 0

mv "$TEMP_FILE" "$FINAL_FILE"
