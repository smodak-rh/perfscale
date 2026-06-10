#!/usr/bin/env python3
import json
import os
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if len(sys.argv) < 4:
    print(
        json.dumps(
            {
                "error": (
                    "usage: get_component_for_pod.py "
                    "<token> <prometheus_host> <pod_name> [namespace] [end_time] [days]"
                )
            }
        )
    )
    sys.exit(1)

token, prom_host, pod = sys.argv[1:4]
namespace = sys.argv[4] if len(sys.argv) > 4 else None
end_time = sys.argv[5] if len(sys.argv) > 5 else None
days = int(sys.argv[6]) if len(sys.argv) > 6 else 1

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}

# Build query with namespace if provided (more accurate)
# Use range query (like list_pods_for_a_particular_task.py) to find deleted pods
# kube_pod_labels can be queried as a range to get historical data
url = f"https://{prom_host}/api/v1/query_range"

# Build the query string
if namespace and namespace != "N/A" and namespace != "":
    query = f'kube_pod_labels{{pod="{pod}",namespace="{namespace}"}}'
else:
    query = f'kube_pod_labels{{pod="{pod}"}}'

# Use range query if end_time is provided (to find deleted pods)
# This matches the approach in list_pods_for_a_particular_task.py
if end_time and end_time != "N/A" and end_time != "" and days:
    try:
        start_time = int(end_time) - (days * 24 * 60 * 60)
        params = {
            "query": query,
            "start": start_time,
            "end": int(end_time),
            # Same step calculation as list_pods_for_a_particular_task.py
            "step": f"{days * 15}s",
        }
    except (ValueError, TypeError):
        # Fallback to instant query if time conversion fails
        url = f"https://{prom_host}/api/v1/query"
        params = {"query": query}
else:
    # Use instant query for current pods
    url = f"https://{prom_host}/api/v1/query"
    params = {"query": query}

try:
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)  # nosec B501
except Exception as exc:
    print(json.dumps({"error": f"request failed: {exc}"}))
    sys.exit(0)

if response.status_code != 200:
    print(json.dumps({"error": f"prometheus returned {response.status_code}"}))
    sys.exit(0)

# Handle instant query response
response_data = response.json()
data = response_data.get("data", {}).get("result", [])

debug_mode = os.environ.get("DEBUG_COMPONENT_LOOKUP") == "1"

if not data:
    # Check if there's an error in the response
    error_info = response_data.get("error")
    if error_info and debug_mode:
        print(
            f"DEBUG: Prometheus query error for pod {pod}: {error_info}",
            file=sys.stderr,
        )
    # No data found - pod might not exist or query returned empty
    if debug_mode:
        print(
            f"DEBUG: No data found for pod {pod} (namespace={namespace}) "
            f"in range query. Query params: {params}",
            file=sys.stderr,
        )
    # Try without namespace filter if namespace was provided
    if namespace and namespace != "N/A" and namespace != "":
        if debug_mode:
            print(
                f"DEBUG: Retrying query without namespace filter for pod {pod}",
                file=sys.stderr,
            )
        query_no_ns = f'kube_pod_labels{{pod="{pod}"}}'
        if end_time and end_time != "N/A" and end_time != "" and days:
            try:
                start_time = int(end_time) - (days * 24 * 60 * 60)
                params_no_ns = {
                    "query": query_no_ns,
                    "start": start_time,
                    "end": int(end_time),
                    "step": f"{days * 15}s",
                }
            except (ValueError, TypeError):
                params_no_ns = {"query": query_no_ns}
        else:
            params_no_ns = {"query": query_no_ns}

        try:
            retry_response = requests.get(
                url,
                headers=headers,
                params=params_no_ns,
                verify=False,
                timeout=30,  # nosec B501
            )
            if retry_response.status_code == 200:
                retry_data = retry_response.json().get("data", {}).get("result", [])
                if retry_data:
                    data = retry_data
                    if debug_mode:
                        print(
                            f"DEBUG: Found data without namespace filter for pod {pod}",
                            file=sys.stderr,
                        )
        except Exception as exc:
            if debug_mode:
                print(
                    f"DEBUG: Exception during retry without namespace: {exc}",
                    file=sys.stderr,
                )

    if not data:
        if debug_mode:
            print(
                f"DEBUG: No data found for pod {pod} after all attempts. Query was: {query}",
                file=sys.stderr,
            )
        print(json.dumps({"component": "N/A", "application": "N/A", "pod": pod}))
        sys.exit(0)

# Get the first result's metric (handles both instant and range query formats)
if isinstance(data, list) and len(data) > 0:
    # Range query: data[0] = {"metric": {...}, "values": [[ts, val], ...]}
    # Instant query: data[0] = {"metric": {...}, "value": [ts, val]}
    metric = data[0].get("metric", {}) if isinstance(data[0], dict) else {}
else:
    metric = {}

# Debug: Always print available label keys when DEBUG is enabled (via wrapper script)
# This helps identify what labels are actually available
if debug_mode:
    all_labels = sorted(metric.keys())
    print(
        f"DEBUG: Available labels for pod {pod} (namespace={namespace}): {all_labels}",
        file=sys.stderr,
    )
    # Also print a sample of label values that might be relevant
    relevant_labels = {
        k: v
        for k, v in metric.items()
        if any(keyword in k.lower() for keyword in ["component", "application", "app", "label"])
    }
    if relevant_labels:
        print(
            f"DEBUG: Relevant labels for pod {pod}: {relevant_labels}",
            file=sys.stderr,
        )

# Expanded list of possible label keys for component
# Prometheus converts special chars: / -> _, . -> _ (sometimes)
# Labels may have "label_" prefix in some setups
# Based on actual Prometheus data: label_appstudio_openshift_io_component
component_keys = [
    "label_appstudio_openshift_io_component",  # Actual format found in Prometheus
    "label_appstudio_redhat_com_component",
    "appstudio_openshift_io_component",
    "appstudio_redhat_com_component",
    "label_appstudio.redhat.com/component",
    "label_appstudio.openshift.io/component",
    "appstudio.redhat.com/component",
    "appstudio.openshift.io/component",
    "label_component",
    "component",
    "label_app_kubernetes_io_component",
    "app.kubernetes.io/component",
    "app_kubernetes_io_component",
]

# Expanded list of possible label keys for application
# Based on actual Prometheus data: label_appstudio_openshift_io_application
application_keys = [
    "label_appstudio_openshift_io_application",  # Actual format found in Prometheus
    "label_appstudio_redhat_com_application",
    "appstudio_openshift_io_application",
    "appstudio_redhat_com_application",
    "label_appstudio.redhat.com/application",
    "label_appstudio.openshift.io/application",
    "appstudio.redhat.com/application",
    "appstudio.openshift.io/application",
    "label_application",
    "application",
    "label_app_kubernetes_io_name",
    "app.kubernetes.io/name",
    "app_kubernetes_io_name",
    "label_app",
    "app",
]


def first_present(mapping, keys):
    for key in keys:
        value = mapping.get(key)
        if value:
            return value
    return "N/A"


component = first_present(metric, component_keys)
application = first_present(metric, application_keys)

# Debug output if enabled
if os.environ.get("DEBUG_COMPONENT_LOOKUP") == "1":
    print(
        f"DEBUG: Found component='{component}', application='{application}' for pod {pod}",
        file=sys.stderr,
    )

output = {
    "component": component,
    "application": application,
    "pod": pod,
}

print(json.dumps(output))
