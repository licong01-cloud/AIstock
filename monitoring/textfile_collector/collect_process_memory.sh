#!/bin/bash
# collect_process_memory.sh — textfile collector for node-exporter
# Collects memory/threads/FDs from /proc for rdagent/python/qrun processes
# Runs every 60s via cron

set -uo pipefail

OUTPUT_DIR="${TEXTFILE_COLLECTOR_DIR:-/mnt/f/Dev/AIstock/monitoring/textfile_collector}"
TEMP_FILE="${OUTPUT_DIR}/process_memory.prom.$$"
FINAL_FILE="${OUTPUT_DIR}/process_memory.prom"

cat > "$TEMP_FILE" <<'HEADER'
# HELP process_memory_vmpeak_bytes Peak virtual memory size in bytes (lifetime max)
# TYPE process_memory_vmpeak_bytes gauge
# HELP process_memory_vmhwm_bytes Peak resident set size in bytes (lifetime max)
# TYPE process_memory_vmhwm_bytes gauge
# HELP process_memory_vmrss_bytes Current resident set size in bytes
# TYPE process_memory_vmrss_bytes gauge
# HELP process_threads Thread count per process group
# TYPE process_threads gauge
# HELP process_open_fds Open file descriptors per process group
# TYPE process_open_fds gauge
HEADER

# Associative arrays for group aggregation
declare -A group_rss group_threads group_fds

for proc_dir in /proc/[0-9]*; do
    pid="${proc_dir##*/}"

    cmdline_file="${proc_dir}/cmdline"
    [ -r "$cmdline_file" ] || continue
    cmdline=$(tr '\0' ' ' < "$cmdline_file" 2>/dev/null) || continue

    # Filter
    case "$cmdline" in
        *rdagent*|*python*|*qrun*) ;;
        *) continue ;;
    esac

    # Determine group name (same logic as process-exporter config)
    group="python"
    case "$cmdline" in
        *rdagent*fin_quant*)  group="rdagent_quant" ;;
        *rdagent*fin_factor*) group="rdagent_factor" ;;
        *qrun*)               group="qlib_qrun" ;;
        *uvicorn*)            group="uvicorn" ;;
    esac

    status_file="${proc_dir}/status"
    [ -r "$status_file" ] || continue

    vmpeak="" vmhwm="" vmrss="" threads=""
    while IFS= read -r line; do
        case "$line" in
            VmPeak:*)  vmpeak=$(echo "$line" | awk '{print $2 * 1024}') ;;
            VmHWM:*)   vmhwm=$(echo "$line" | awk '{print $2 * 1024}') ;;
            VmRSS:*)   vmrss=$(echo "$line" | awk '{print $2 * 1024}') ;;
            Threads:*) threads=$(echo "$line" | awk '{print $2}') ;;
        esac
    done < "$status_file"

    [ -z "$vmrss" ] && continue

    # Count FDs
    fds=$(ls -1 "${proc_dir}/fd" 2>/dev/null | wc -l)

    # Per-PID metrics (for VmHWM peak tracking)
    cmdline_short=$(echo "$cmdline" | head -c 100 | sed 's/\\/\\\\/g; s/"/\\"/g')
    echo "process_memory_vmpeak_bytes{pid=\"${pid}\",group=\"${group}\",cmdline_short=\"${cmdline_short}\"} ${vmpeak:-0}" >> "$TEMP_FILE"
    echo "process_memory_vmhwm_bytes{pid=\"${pid}\",group=\"${group}\",cmdline_short=\"${cmdline_short}\"} ${vmhwm:-0}" >> "$TEMP_FILE"
    echo "process_memory_vmrss_bytes{pid=\"${pid}\",group=\"${group}\",cmdline_short=\"${cmdline_short}\"} ${vmrss:-0}" >> "$TEMP_FILE"

    # Accumulate for group-level aggregates
    group_rss[$group]=$(( ${group_rss[$group]:-0} + ${vmrss:-0} ))
    group_threads[$group]=$(( ${group_threads[$group]:-0} + ${threads:-0} ))
    group_fds[$group]=$(( ${group_fds[$group]:-0} + ${fds:-0} ))
done

# Write group-level aggregates
for g in "${!group_rss[@]}"; do
    echo "process_threads{group=\"${g}\"} ${group_threads[$g]}" >> "$TEMP_FILE"
    echo "process_open_fds{group=\"${g}\"} ${group_fds[$g]}" >> "$TEMP_FILE"
done

mv "$TEMP_FILE" "$FINAL_FILE"
