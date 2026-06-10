#!/bin/bash
# ShellCheck-clean, bash 3.2 compatible

set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <last_num_days> [--csv] [--table] [--raw] [--debug]" >&2
    echo "  --csv   : Output as CSV (default, pipeable)" >&2
    echo "  --table : Output as readable table format" >&2
    echo "  --raw   : Output raw CSV without formatting" >&2
    exit 1
fi

LAST_DAYS="$1"
shift

OUTPUT_MODE="--csv"
TABLE_FORMAT=0
RAW_MODE=0
DEBUG=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --csv) OUTPUT_MODE="--csv" ;;
        --table) OUTPUT_MODE="--csv"; TABLE_FORMAT=1 ;;
        --raw) OUTPUT_MODE="--csv"; RAW_MODE=1 ;;
        --debug) DEBUG=1 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
    shift
done

#TASK_NAME="buildah"
#STEPS="step-build step-push step-prepare-sboms step-upload-sbom"
#STEPS="step-build step-push step-sbom-syft-generate step-prepare-sboms step-upload-sbom"
#STEPS="step-build"
#STEPS="step-build step-push"

# Get all contexts or use specific one for testing
CONTEXTS="$(kubectl config get-contexts -o name 2>/dev/null | xargs || echo 'default/api-stone-prd-rh01-pg1f-p1-openshiftapps-com:6443/smodak')"
#CONTEXTS="default/api-stone-prd-rh01-pg1f-p1-openshiftapps-com:6443/smodak default/api-stone-prod-p01-wcfb-p1-openshiftapps-com:6443/smodak default/api-kflux-prd-rh02-0fk9-p1-openshiftapps-com:6443/smodak default/api-kflux-prd-rh03-nnv1-p1-openshiftapps-com:6443/smodak default/api-stone-prod-p02-hjvn-p1-openshiftapps-com:6443/smodak default/api-kflux-stg-es01-21tc-p1-openshiftapps-com:6443/smodak"
#CONTEXTS="default/api-stone-prd-rh01-pg1f-p1-openshiftapps-com:6443/smodak"
#CONTEXTS="default/api-stone-prod-p01-wcfb-p1-openshiftapps-com:6443/smodak"
#CONTEXTS="default/api-stone-prd-rh01-pg1f-p1-openshiftapps-com:6443/smodak default/api-stone-prod-p01-wcfb-p1-openshiftapps-com:6443/smodak default/api-kflux-prd-rh02-0fk9-p1-openshiftapps-com:6443/smodak default/api-kflux-stg-es01-21tc-p1-openshiftapps-com:6443/smodak"

# Create temporary file for CSV output
TMP_CSV=$(mktemp)
trap 'rm -f "$TMP_CSV"' EXIT

# CSV header matching the output format: cluster, task, step, pod_max_mem, namespace_max_mem, component_max_mem, application_max_mem, mem_max_mb, mem_p95_mb, mem_p90_mb, mem_median_mb, pod_max_cpu, namespace_max_cpu, component_max_cpu, application_max_cpu, cpu_max, cpu_p95, cpu_p90, cpu_median
echo '"cluster", "task", "step", "pod_max_mem", "namespace_max_mem", "component_max_mem", "application_max_mem", "mem_max_mb", "mem_p95_mb", "mem_p90_mb", "mem_median_mb", "pod_max_cpu", "namespace_max_cpu", "component_max_cpu", "application_max_cpu", "cpu_max", "cpu_p95", "cpu_p90", "cpu_median"' >> "$TMP_CSV"

for ctx in ${CONTEXTS}; do
    kubectl config use-context "$ctx" >/dev/null 2>&1 || continue
    [ "$DEBUG" -eq 1 ] && echo "DEBUG(parent): context=$ctx" >&2

    for step in ${STEPS}; do
        ./wrapper_for_promql.sh \
            "$TASK_NAME" \
            "$step" \
            "$LAST_DAYS" \
            "$OUTPUT_MODE" \
            "$DEBUG" >> "$TMP_CSV"
    done
done

# Output formatting
if [ "$RAW_MODE" -eq 1 ]; then
    # Raw CSV output (for piping)
    cat "$TMP_CSV"
elif [ "$TABLE_FORMAT" -eq 1 ] || [ -t 1 ]; then
    # Table format if explicitly requested OR if output is to terminal
    python3 "$(dirname "$0")/format_csv_table.py" < "$TMP_CSV"
else
    # If piped, output raw CSV for compatibility
    cat "$TMP_CSV"
fi

