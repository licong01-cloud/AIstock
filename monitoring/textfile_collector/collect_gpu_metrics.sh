#!/bin/bash
# collect_gpu_metrics.sh — textfile collector for GPU metrics via nvidia-smi
# Collects temperature, fan speed, power, memory usage, GPU utilization
# Runs every 60s via cron, writes .prom file for node-exporter textfile collector

set -euo pipefail

OUTPUT_DIR="${TEXTFILE_COLLECTOR_DIR:-/mnt/f/Dev/AIstock/monitoring/textfile_collector}"
TEMP_FILE="${OUTPUT_DIR}/gpu_metrics.prom.$$"
FINAL_FILE="${OUTPUT_DIR}/gpu_metrics.prom"

# Check nvidia-smi exists
if ! command -v nvidia-smi &>/dev/null; then
    exit 0
fi

# Query all GPUs: index, name, temp, fan, power_draw, power_limit, mem_used, mem_total, util_gpu, util_mem
data=$(nvidia-smi --query-gpu=index,name,temperature.gpu,fan.speed,power.draw,power.limit,memory.used,memory.total,utilization.gpu,utilization.memory --format=csv,noheader,nounits 2>/dev/null) || exit 0

cat > "$TEMP_FILE" <<'HEADER'
# HELP gpu_temperature_celsius GPU core temperature in Celsius
# TYPE gpu_temperature_celsius gauge
# HELP gpu_fan_speed_percent GPU fan speed as percentage of max
# TYPE gpu_fan_speed_percent gauge
# HELP gpu_power_draw_watts GPU power draw in watts
# TYPE gpu_power_draw_watts gauge
# HELP gpu_power_limit_watts GPU power limit in watts
# TYPE gpu_power_limit_watts gauge
# HELP gpu_memory_used_bytes GPU memory used in bytes
# TYPE gpu_memory_used_bytes gauge
# HELP gpu_memory_total_bytes GPU total memory in bytes
# TYPE gpu_memory_total_bytes gauge
# HELP gpu_utilization_percent GPU core utilization percentage
# TYPE gpu_utilization_percent gauge
# HELP gpu_memory_utilization_percent GPU memory controller utilization percentage
# TYPE gpu_memory_utilization_percent gauge
HEADER

while IFS= read -r line; do
    # Split by ", " delimiter
    idx=$(echo "$line" | cut -d',' -f1 | xargs)
    name=$(echo "$line" | cut -d',' -f2 | xargs)
    temp=$(echo "$line" | cut -d',' -f3 | xargs)
    fan=$(echo "$line" | cut -d',' -f4 | xargs)
    power=$(echo "$line" | cut -d',' -f5 | xargs)
    plimit=$(echo "$line" | cut -d',' -f6 | xargs)
    mem_used=$(echo "$line" | cut -d',' -f7 | xargs)
    mem_total=$(echo "$line" | cut -d',' -f8 | xargs)
    util_gpu=$(echo "$line" | cut -d',' -f9 | xargs)
    util_mem=$(echo "$line" | cut -d',' -f10 | xargs)

    # Convert [Not Supported] to 0
    [ "$fan" = "[Not Supported]" ] && fan=0
    # Memory MiB -> bytes
    mem_used_bytes=$((mem_used * 1048576))
    mem_total_bytes=$((mem_total * 1048576))

    echo "gpu_temperature_celsius{gpu=\"${idx}\",gpu_name=\"${name}\"} ${temp}" >> "$TEMP_FILE"
    echo "gpu_fan_speed_percent{gpu=\"${idx}\",gpu_name=\"${name}\"} ${fan}" >> "$TEMP_FILE"
    echo "gpu_power_draw_watts{gpu=\"${idx}\",gpu_name=\"${name}\"} ${power}" >> "$TEMP_FILE"
    echo "gpu_power_limit_watts{gpu=\"${idx}\",gpu_name=\"${name}\"} ${plimit}" >> "$TEMP_FILE"
    echo "gpu_memory_used_bytes{gpu=\"${idx}\",gpu_name=\"${name}\"} ${mem_used_bytes}" >> "$TEMP_FILE"
    echo "gpu_memory_total_bytes{gpu=\"${idx}\",gpu_name=\"${name}\"} ${mem_total_bytes}" >> "$TEMP_FILE"
    echo "gpu_utilization_percent{gpu=\"${idx}\",gpu_name=\"${name}\"} ${util_gpu}" >> "$TEMP_FILE"
    echo "gpu_memory_utilization_percent{gpu=\"${idx}\",gpu_name=\"${name}\"} ${util_mem}" >> "$TEMP_FILE"
done <<< "$data"

mv "$TEMP_FILE" "$FINAL_FILE"
