#!/bin/bash
# ShellCheck-clean, bash 3.2 compatible

set -u  # NEVER -e

TASK="$1"
STEP="$2"
DAYS="$3"
# shellcheck disable=SC2034
MODE="$4"  # Currently unused, reserved for future output format support
DEBUG_FLAG="${5:-0}"

DEBUG=0
[ "$DEBUG_FLAG" -eq 1 ] && DEBUG=1

log() { [ "$DEBUG" -eq 1 ] && echo "DEBUG(child): $*" >&2; }

CLUSTER="$(oc config current-context | sed -E 's#.*/api-([^-]+-[^-]+-[^-]+).*#\1#')"
TOKEN="$(oc whoami --show-token)"
PROM="$(oc -n openshift-monitoring get route prometheus-k8s --no-headers | awk '{print $2}')"

END="$(date +%s)"
LOOKBACK_SECONDS=$(( DAYS * 86400 ))
START=$(( END - LOOKBACK_SECONDS ))
RANGE="${DAYS}d"

query() {
    local qry="$1"
    log "Query: $qry"
    local result
    local retry_count=0
    local max_retries=10

    # Retry logic for handling transient interruptions
    while [ "$retry_count" -le "$max_retries" ]; do
        # Capture output with error handling for interrupted system calls
        result="$(python3 query_prometheus_range.py "$TOKEN" "$PROM" "$qry" "$START" "$END" 2>&1)" || {
            local status=$?
            if [ "$retry_count" -lt "$max_retries" ]; then
                log "Query failed (status=$status), retrying ($((retry_count + 1))/$max_retries)..."
                sleep 1
                retry_count=$((retry_count + 1))
                continue
            else
                log "Query failed after $max_retries retries"
                echo '{}'
                return
            fi
        }

        # Check for empty result
        if [ -z "$result" ]; then
            if [ "$retry_count" -lt "$max_retries" ]; then
                log "Query returned empty result, retrying ($((retry_count + 1))/$max_retries)..."
                sleep 1
                retry_count=$((retry_count + 1))
                continue
            else
                log "Query returned empty result after retries"
                echo '{}'
                return
            fi
        fi

        # Validate JSON is complete (check if jq can parse it)
        if ! echo "$result" | jq empty >/dev/null 2>&1; then
            if [ "$retry_count" -lt "$max_retries" ]; then
                log "Query returned incomplete JSON, retrying ($((retry_count + 1))/$max_retries)..."
                sleep 1
                retry_count=$((retry_count + 1))
                continue
            else
                log "Query returned invalid JSON after retries"
                echo '{}'
                return
            fi
        fi

        # Check if result contains error
        if echo "$result" | jq -e '.error // .status' >/dev/null 2>&1; then
            local error_type
            error_type="$(echo "$result" | jq -r '.error.type // .status // "unknown"')"
            if [ "$error_type" != "success" ] && [ -n "$error_type" ]; then
                log "Query returned error: $result"
            fi
        fi

        # Success - output result
        # Suppress "Interrupted system call" errors from echo (they're usually harmless)
        echo "$result" 2>/dev/null || echo "$result"
        return 0
    done

    # Fallback if all retries failed
    echo '{}'
}

bytes_to_mb() {
    v="${1%%.*}"
    [ -z "$v" ] && v=0
    echo $(( v / 1024 / 1024 ))
}

# ------------------------------------------------------------
# OPTIMIZED: Batch pods to avoid regex explosion with large pod counts
#
# SAFETY GUARANTEES:
# 1. Pod list is obtained by filtering kube_pod_labels with label_tekton_dev_task="$TASK"
#    This ensures we ONLY get pods belonging to the specified task
# 2. All metric queries use pod=~"(pod1|pod2|...)" with exact pod names from step 1
# 3. All returned pod names are validated against the original task pod list
# 4. This ensures max/percentile calculations are ONLY from pods of the specified task,
#    not from other tasks that might have the same step name
# ------------------------------------------------------------

# Get list of pods for this task (filtered by task label)
# list_pods_for_a_particular_task.py argv[5] is lookback seconds (not days)
log "Getting pods for task=$TASK..."
POD_LIST="$(python3 list_pods_for_a_particular_task.py "$TOKEN" "$PROM" "$TASK" "$END" "$LOOKBACK_SECONDS" 2>/dev/null | jq -r '.data.result[].metric.pod' 2>/dev/null | sort -u)"

# Convert to array (bash 3.2 compatible - no associative arrays)
pods=()
while IFS= read -r pod; do
    [ -n "$pod" ] && pods+=("$pod")
done <<< "$POD_LIST"

POD_COUNT="${#pods[@]}"
log "Found $POD_COUNT pods for task=$TASK (validated by task label: label_tekton_dev_task=\"$TASK\")"

# Helper function to check if pod is in our task pod list (bash 3.2 compatible)
pod_in_task() {
    local check_pod="$1"
    local p
    for p in "${pods[@]}"; do
        [ "$p" = "$check_pod" ] && return 0
    done
    return 1
}

if [ "$POD_COUNT" -eq 0 ]; then
    log "No pods found, exiting"
    exit 0
fi

# Batch size - keep it small to avoid URL length limits (50 is safe)
BATCH_SIZE=50

# Escape a pod name for use in PromQL regex (pod=~"...").
# Prevents . | ( ) [ ] * + ? etc. in pod names from matching wrong pods.
escape_pod_regex() {
    echo "$1" | sed -e 's/\\/\\\\/g' -e 's/\./\\\./g' -e 's/\*/\\*/g' -e 's/+/\\+/g' \
        -e 's/?/\\?/g' -e 's/\[/\\[/g' -e 's/\]/\\]/g' -e 's/(/\\(/g' -e 's/)/\\)/g' \
        -e 's/{/\\{/g' -e 's/}/\\}/g' -e 's/\^/\\^/g' -e 's/\$/\\$/g' -e 's/|/\\|/g'
}

# Global accumulators
max_overall=0
max_overall_pod=""
max_overall_ns="N/A"
perc95_overall=0
perc90_overall=0
median_overall=0

cpu_max_overall=0
cpu_max_overall_pod=""
cpu_max_overall_ns="N/A"
cpu_p95_overall=0
cpu_p90_overall=0
cpu_median_overall=0

# Process pods in batches
total="$POD_COUNT"
start=0

log "Processing $total pods in batches of $BATCH_SIZE..."

while [ "$start" -lt "$total" ]; do
    end=$(( start + BATCH_SIZE ))
    [ "$end" -gt "$total" ] && end=$total

    # Create regex for this batch: escape each pod name so literal chars (e.g. . | ( )) don't match wrong pods
    batch_pods_escaped=""
    idx=0
    while [ "$idx" -lt "$(( end - start ))" ]; do
        p="${pods[$(( start + idx ))]}"
        [ -n "$p" ] || { idx=$(( idx + 1 )); continue; }
        escaped="$(escape_pod_regex "$p")"
        [ -n "$batch_pods_escaped" ] && batch_pods_escaped="${batch_pods_escaped}|"
        batch_pods_escaped="${batch_pods_escaped}${escaped}"
        idx=$(( idx + 1 ))
    done
    batch_pods="$batch_pods_escaped"
    batch_count=$((end - start))
    log "Processing batch $((start/BATCH_SIZE + 1)): pods $start to $((end-1)) ($batch_count pods)"

    # ------------------------------------------------------------
    # MEMORY - Max
    # Use container_memory_working_set_bytes (actual usage) instead of
    # container_memory_max_usage_bytes (which reflects limits, not actual usage)
    # ------------------------------------------------------------
    max_query="max_over_time(container_memory_working_set_bytes{container=\"$STEP\",pod=~\"($batch_pods)\",namespace=~\".*-tenant\"}[$RANGE])"
    max_json="$(query "$max_query")"

    max_entry="$(echo "$max_json" | jq -r '
      [.data.result[]? |
        {
          value: ([.values[][1] | tonumber | floor | select(. > 0)] | max // 0),
          pod: .metric.pod,
          namespace: .metric.namespace
        }
      ] |
      if length == 0 then
        {value: 0, pod: "", namespace: "N/A"}
      else
        max_by(.value)
      end
    ')"

    # Validate that the returned pod is in our task pod list
    returned_pod="$(echo "$max_entry" | jq -r '.pod // ""')"
    if [ -n "$returned_pod" ] && ! pod_in_task "$returned_pod"; then
        log "WARNING: Pod $returned_pod not in task pod list - skipping"
        batch_max_bytes=0
        batch_max_mb=0
    else
        batch_max_bytes="$(echo "$max_entry" | jq -r '.value // 0')"
        batch_max_mb=$(( batch_max_bytes / 1024 / 1024 ))
    fi

    if [ "$batch_max_mb" -gt "$max_overall" ]; then
        max_overall="$batch_max_mb"
        max_overall_pod="$(echo "$max_entry" | jq -r '.pod // ""')"
        max_overall_ns="$(echo "$max_entry" | jq -r '.namespace // "N/A"')"
    fi

    # ------------------------------------------------------------
    # MEMORY - Percentiles (actual percentile over all pod values, not max)
    # ------------------------------------------------------------
    for q in 0.95 0.90 0.50; do
        p_query="quantile_over_time($q,container_memory_working_set_bytes{container=\"$STEP\",pod=~\"($batch_pods)\",namespace=~\".*-tenant\"}[$RANGE])"
        p_json="$(query "$p_query")"
        # Compute actual percentile: collect all values, sort, take element at (length-1)*q
        p_bytes="$(echo "$p_json" | jq --argjson pct "$q" -r '[.data.result[]?.values[][1] | tonumber | select(. > 0)] | sort | if length > 0 then .[(((length - 1) * $pct) | floor)] else 0 end')"
        p_mb=$(( p_bytes / 1024 / 1024 ))

        case "$q" in
            0.95) [ "$p_mb" -gt "$perc95_overall" ] && perc95_overall="$p_mb" ;;
            0.90) [ "$p_mb" -gt "$perc90_overall" ] && perc90_overall="$p_mb" ;;
            0.50) [ "$p_mb" -gt "$median_overall" ] && median_overall="$p_mb" ;;
        esac
    done

    # ------------------------------------------------------------
    # CPU - Max
    # ------------------------------------------------------------
    cpu_max_query="max_over_time(rate(container_cpu_usage_seconds_total{container=\"$STEP\",pod=~\"($batch_pods)\",namespace=~\".*-tenant\"}[5m])[$RANGE:5m])"
    cpu_max_json="$(query "$cpu_max_query")"

    cpu_max_entry="$(echo "$cpu_max_json" | jq -r '
      [.data.result[]? |
        {
          value: ([.values[][1] | tonumber | select(. > 0)] | max // 0),
          pod: .metric.pod,
          namespace: .metric.namespace
        }
      ] |
      if length == 0 then
        {value: 0, pod: "", namespace: "N/A"}
      else
        max_by(.value)
      end
    ')"

    # Validate that the returned pod is in our task pod list
    returned_cpu_pod="$(echo "$cpu_max_entry" | jq -r '.pod // ""')"
    if [ -n "$returned_cpu_pod" ] && ! pod_in_task "$returned_cpu_pod"; then
        log "WARNING: Pod $returned_cpu_pod not in task pod list - skipping"
        cpu_max_cores=0
    else
        cpu_max_cores="$(echo "$cpu_max_entry" | jq -r '.value // 0')"
    fi
    # Convert to millicores, handling empty values
    if [ -n "$cpu_max_cores" ] && [ "$cpu_max_cores" != "0" ]; then
        cpu_max_millicores=$(echo "$cpu_max_cores * 1000" | bc 2>/dev/null | cut -d. -f1)
        [ -z "$cpu_max_millicores" ] && cpu_max_millicores=0
    else
        cpu_max_millicores=0
    fi

    if [ "$cpu_max_millicores" -gt "$cpu_max_overall" ]; then
        cpu_max_overall="$cpu_max_millicores"
        cpu_max_overall_pod="$(echo "$cpu_max_entry" | jq -r '.pod // ""')"
        cpu_max_overall_ns="$(echo "$cpu_max_entry" | jq -r '.namespace // "N/A"')"
    fi

    # ------------------------------------------------------------
    # CPU - Percentiles (actual percentile over all pod values, not max)
    # ------------------------------------------------------------
    for q in 0.95 0.90 0.50; do
        cpu_p_query="quantile_over_time($q,rate(container_cpu_usage_seconds_total{container=\"$STEP\",pod=~\"($batch_pods)\",namespace=~\".*-tenant\"}[5m])[$RANGE:5m])"
        cpu_p_json="$(query "$cpu_p_query")"
        cpu_p_cores="$(echo "$cpu_p_json" | jq --argjson pct "$q" -r '[.data.result[]?.values[][1] | tonumber | select(. > 0)] | sort | if length > 0 then .[(((length - 1) * $pct) | floor)] else 0 end')"
        # Convert to millicores, handling empty values
        cpu_p_millicores=0
        if [ -n "$cpu_p_cores" ] && [ "$cpu_p_cores" != "0" ]; then
            cpu_p_millicores=$(echo "$cpu_p_cores * 1000" | bc 2>/dev/null | cut -d. -f1)
            [ -z "$cpu_p_millicores" ] && cpu_p_millicores=0
        fi

        case "$q" in
            0.95) [ "$cpu_p_millicores" -gt "$cpu_p95_overall" ] && cpu_p95_overall="$cpu_p_millicores" ;;
            0.90) [ "$cpu_p_millicores" -gt "$cpu_p90_overall" ] && cpu_p90_overall="$cpu_p_millicores" ;;
            0.50) [ "$cpu_p_millicores" -gt "$cpu_median_overall" ] && cpu_median_overall="$cpu_p_millicores" ;;
        esac
    done

    start=$end
done

# Set final values
MEM_MAX_POD="$max_overall_pod"
MEM_MAX_NS="$max_overall_ns"
MEM_MAX_MB="$max_overall"
MEM_P95_MB="$perc95_overall"
MEM_P90_MB="$perc90_overall"
MEM_MED_MB="$median_overall"

CPU_MAX_POD="$cpu_max_overall_pod"
CPU_MAX_NS="$cpu_max_overall_ns"
CPU_MAX_M="${cpu_max_overall}m"
CPU_P95_M="${cpu_p95_overall}m"
CPU_P90_M="${cpu_p90_overall}m"
CPU_MED_M="${cpu_median_overall}m"

# Get component and application info for the max memory pod
# Pass END and DAYS to use range query (for deleted pods)
if [ -n "$MEM_MAX_POD" ] && [ "$MEM_MAX_POD" != "" ] && [ "$MEM_MAX_POD" != "N/A" ]; then
    log "Looking up component/application for memory pod: $MEM_MAX_POD in namespace: $MEM_MAX_NS"
    # Enable debug mode for component lookup if DEBUG is enabled
    [ "$DEBUG" -eq 1 ] && export DEBUG_COMPONENT_LOOKUP=1
    # Capture stdout (JSON) separately from stderr (debug messages)
    # Use process substitution to redirect stderr to log while keeping stdout clean
    COMP_INFO="$(python3 get_component_for_pod.py "$TOKEN" "$PROM" "$MEM_MAX_POD" "$MEM_MAX_NS" "$END" "$DAYS" 2> >(while IFS= read -r line; do log "$line"; done))"
    [ "$DEBUG" -eq 1 ] && unset DEBUG_COMPONENT_LOOKUP
    # Extract JSON from output (may have debug messages mixed in)
    JSON_LINE="$(echo "$COMP_INFO" | grep -E '^\{.*\}$' | head -1)"
    if [ -n "$JSON_LINE" ] && echo "$JSON_LINE" | jq empty >/dev/null 2>&1; then
        MEM_MAX_COMP="$(echo "$JSON_LINE" | jq -r '.component // "N/A"')"
        MEM_MAX_APP="$(echo "$JSON_LINE" | jq -r '.application // "N/A"')"
        log "Memory pod component: $MEM_MAX_COMP, application: $MEM_MAX_APP"
    else
        log "Failed to parse component info for memory pod. Output: $COMP_INFO"
        MEM_MAX_COMP="N/A"
        MEM_MAX_APP="N/A"
    fi
else
    MEM_MAX_COMP="N/A"
    MEM_MAX_APP="N/A"
fi

# Get component and application info for the max CPU pod
# Pass END and DAYS to use range query (for deleted pods)
if [ -n "$CPU_MAX_POD" ] && [ "$CPU_MAX_POD" != "" ] && [ "$CPU_MAX_POD" != "N/A" ]; then
    log "Looking up component/application for CPU pod: $CPU_MAX_POD in namespace: $CPU_MAX_NS"
    # Enable debug mode for component lookup if DEBUG is enabled
    [ "$DEBUG" -eq 1 ] && export DEBUG_COMPONENT_LOOKUP=1
    # Capture stdout (JSON) separately from stderr (debug messages)
    # Use process substitution to redirect stderr to log while keeping stdout clean
    CPU_COMP_INFO="$(python3 get_component_for_pod.py "$TOKEN" "$PROM" "$CPU_MAX_POD" "$CPU_MAX_NS" "$END" "$DAYS" 2> >(while IFS= read -r line; do log "$line"; done))"
    [ "$DEBUG" -eq 1 ] && unset DEBUG_COMPONENT_LOOKUP
    # Extract JSON from output (may have debug messages mixed in)
    JSON_LINE="$(echo "$CPU_COMP_INFO" | grep -E '^\{.*\}$' | head -1)"
    if [ -n "$JSON_LINE" ] && echo "$JSON_LINE" | jq empty >/dev/null 2>&1; then
        CPU_MAX_COMP="$(echo "$JSON_LINE" | jq -r '.component // "N/A"')"
        CPU_MAX_APP="$(echo "$JSON_LINE" | jq -r '.application // "N/A"')"
        log "CPU pod component: $CPU_MAX_COMP, application: $CPU_MAX_APP"
    else
        log "Failed to parse component info for CPU pod. Output: $CPU_COMP_INFO"
        CPU_MAX_COMP="N/A"
        CPU_MAX_APP="N/A"
    fi
else
    CPU_MAX_COMP="N/A"
    CPU_MAX_APP="N/A"
fi

# ------------------------------------------------------------
# OUTPUT (ALWAYS ONE LINE)
# ------------------------------------------------------------
# Format: cluster, task, step, pod_max_mem, namespace_max_mem, component_max_mem, application_max_mem, mem_max_mb, mem_p95_mb, mem_p90_mb, mem_median_mb, pod_max_cpu, namespace_max_cpu, component_max_cpu, application_max_cpu, cpu_max, cpu_p95, cpu_p90, cpu_median

printf '"%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s"\n' \
  "$CLUSTER" "$TASK" "$STEP" \
  "$MEM_MAX_POD" "$MEM_MAX_NS" "$MEM_MAX_COMP" "$MEM_MAX_APP" \
  "$MEM_MAX_MB" "$MEM_P95_MB" "$MEM_P90_MB" "$MEM_MED_MB" \
  "$CPU_MAX_POD" "$CPU_MAX_NS" "$CPU_MAX_COMP" "$CPU_MAX_APP" \
  "$CPU_MAX_M" "$CPU_P95_M" "$CPU_P90_M" "$CPU_MED_M"
