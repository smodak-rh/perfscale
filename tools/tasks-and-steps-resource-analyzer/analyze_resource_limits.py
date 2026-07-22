#!/usr/bin/env python3
"""
Analyze resource consumption data and provide recommendations for resource limits.

Usage:
    # From piped input:
    ./wrapper_for_promql_for_all_clusters.sh 7 --csv | ./analyze_resource_limits.py

    # From YAML file (auto-runs data collection):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml
    ./analyze_resource_limits.py --file https://github.com/.../buildah.yaml

    # Update YAML file with recommendations:
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --update
"""

import argparse
import csv
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, Thread

try:
    import requests
    import yaml
except ImportError:
    print(
        "Error: Missing required library. Install with: pip install requests pyyaml",
        file=sys.stderr,
    )
    sys.exit(1)


def convert_github_url_to_raw(url):
    """Convert GitHub blob URL to raw content URL."""
    # Convert blob URL to raw URL
    # https://github.com/user/repo/blob/branch/path -> https://raw.githubusercontent.com/user/repo/branch/path
    pattern = r"https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)"
    match = re.match(pattern, url)
    if match:
        user, repo, branch, path = match.groups()
        return f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{path}"
    return url


def fetch_yaml_content(file_path_or_url):
    """Fetch YAML content from file path or URL."""
    if file_path_or_url.startswith("http://") or file_path_or_url.startswith("https://"):
        url = convert_github_url_to_raw(file_path_or_url)
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return yaml.safe_load(response.text), url
        except requests.RequestException as e:
            print(f"Error fetching URL {url}: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        path = Path(file_path_or_url)
        if not path.exists():
            print(f"Error: File not found: {file_path_or_url}", file=sys.stderr)
            sys.exit(1)
        with open(path) as f:
            return yaml.safe_load(f), str(path.absolute())


def extract_task_info(yaml_content):
    """Extract task name, step names, and current resource limits from Tekton Task YAML."""
    task_name = yaml_content.get("metadata", {}).get("name", "")
    steps = []
    current_resources = {}

    # Get default resources from stepTemplate
    # Support both Tekton v1 (computeResources) and v1beta1 (resources) field names
    step_template = yaml_content.get("spec", {}).get("stepTemplate", {})
    default_resources = step_template.get("computeResources") or step_template.get("resources", {})
    default_mem_req = default_resources.get("requests", {}).get("memory", "")
    default_cpu_req = default_resources.get("requests", {}).get("cpu", "")
    default_mem_lim = default_resources.get("limits", {}).get("memory", "")
    default_cpu_lim = default_resources.get("limits", {}).get("cpu", "")

    # Extract step names and current resources from spec.steps
    for step in yaml_content.get("spec", {}).get("steps", []):
        step_name = step.get("name", "")
        if step_name:
            steps.append(step_name)

            # Get current resources for this step (use defaults if not specified)
            # Support both Tekton v1 (computeResources) and v1beta1 (resources) field names
            step_resources = step.get("computeResources") or step.get("resources", {})
            step_req = step_resources.get("requests", {})
            step_lim = step_resources.get("limits", {})

            # Get values, using None if not set (to distinguish from empty string)
            mem_req = (
                step_req.get("memory")
                if "memory" in step_req
                else (default_mem_req if default_mem_req else None)
            )
            cpu_req = (
                step_req.get("cpu")
                if "cpu" in step_req
                else (default_cpu_req if default_cpu_req else None)
            )
            mem_lim = (
                step_lim.get("memory")
                if "memory" in step_lim
                else (default_mem_lim if default_mem_lim else None)
            )
            cpu_lim = (
                step_lim.get("cpu")
                if "cpu" in step_lim
                else (default_cpu_lim if default_cpu_lim else None)
            )

            current_resources[step_name] = {
                "requests": {
                    "memory": mem_req,
                    "cpu": cpu_req,
                },
                "limits": {
                    "memory": mem_lim,
                    "cpu": cpu_lim,
                },
            }

    return task_name, steps, default_resources, current_resources


def normalize_step_name_for_compare(name):
    """Strip Tekton step- prefix so YAML names match CSV step column."""
    if not name:
        return ""
    s = str(name).strip()
    if s.startswith("step-"):
        return s[5:]
    return s


def compute_steps_missing_observability(declared_steps, by_step):
    """Declared YAML steps that have no rows in aggregated observability data.

    Args:
        declared_steps: Iterable of step names as in YAML (with or without step- prefix)
        by_step: defaultdict or dict keyed by step name as in CSV (no step- prefix)

    Returns:
        Sorted list of step names (no step- prefix) missing from data.
    """
    declared = {
        normalize_step_name_for_compare(s)
        for s in (declared_steps or [])
        if normalize_step_name_for_compare(s)
    }
    seen = set(by_step.keys()) if by_step else set()
    return sorted(declared - seen)


def _html_steps_missing_observability_banner(steps_missing):
    """HTML notice when YAML declares steps that did not appear in aggregated metrics."""
    if not steps_missing:
        return ""
    items = "\n".join(f"        <li>{html.escape(s)}</li>" for s in steps_missing)
    return (
        '    <div style="background:#fff3cd;border:1px solid #ffc107;'
        'padding:12px 16px;margin:16px 0;border-radius:4px;">\n'
        "        <strong>Steps in YAML with no observability data in this run</strong>\n"
        f'        <ul style="margin:8px 0 0 16px;">{items}\n        </ul>\n'
        '        <p style="margin:8px 0 0 0;font-size:0.95em;color:#555;">'
        "Recommendations only include steps with Prometheus samples for "
        "<code>container=&quot;step-&lt;name&gt;&quot;</code> in the analysis window.</p>\n"
        "    </div>\n"
    )


# ---------------------------------------------------------------------------
# Finding 2: Cluster data coverage window
# ---------------------------------------------------------------------------


def compute_cluster_coverage_report(detailed_executions, days_requested):
    """Compute the actual data coverage window per cluster from collected executions.

    Returns a dict keyed by cluster display name:
        {cluster: {'oldest_date': str, 'days_covered': float,
                   'meets_requested': bool, 'pod_count': int}}
    """
    if not detailed_executions:
        return {}

    now = datetime.now()
    by_cluster = defaultdict(list)
    for e in detailed_executions:
        cluster = e.get("cluster", "unknown")
        ts = e.get("timestamp", "")
        if ts:
            by_cluster[cluster].append(ts)

    report = {}
    for cluster, timestamps in by_cluster.items():
        valid_ts = []
        for ts in timestamps:
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                valid_ts.append(dt)
            except (ValueError, TypeError):
                pass
        if not valid_ts:
            continue
        oldest = min(valid_ts)
        days_covered = (now - oldest).total_seconds() / 86400.0
        report[cluster] = {
            "oldest_date": oldest.strftime("%Y-%m-%d %H:%M:%S"),
            "days_covered": round(days_covered, 1),
            "meets_requested": days_covered >= (days_requested * 0.9),  # 10% tolerance
            "pod_count": len(timestamps),
        }
    return report


def _html_cluster_coverage_banner(coverage_report, days_requested):
    """HTML banner showing actual data coverage per cluster."""
    if not coverage_report:
        return ""

    rows = ""
    has_warning = False
    for cluster in sorted(coverage_report.keys()):
        info = coverage_report[cluster]
        days = info["days_covered"]
        meets = info["meets_requested"]
        oldest = info["oldest_date"]
        pods = info["pod_count"]
        if not meets:
            has_warning = True
            icon = "&#9888;"  # warning triangle
            row_style = "color:#856404;background:#fff3cd;"
        else:
            icon = "&#10003;"  # check mark
            row_style = "color:#155724;"
        rows += (
            f'<tr style="{row_style}">'
            f'<td style="padding:4px 10px;">{html.escape(cluster)}</td>'
            f'<td style="padding:4px 10px;">{icon} {days:.1f} days</td>'
            f'<td style="padding:4px 10px;">{oldest}</td>'
            f'<td style="padding:4px 10px;">{pods}</td>'
            f"</tr>\n"
        )

    border_color = "#ffc107" if has_warning else "#28a745"
    bg_color = "#fff3cd" if has_warning else "#d4edda"
    heading = (
        f"&#9888; Some clusters returned less data than the requested {days_requested} days — "
        "statistics may under-represent rare heavy workloads on those clusters."
        if has_warning
        else f"&#10003; All clusters returned data covering the full {days_requested}-day window."
    )

    return (
        f'    <div style="background:{bg_color};border:1px solid {border_color};padding:12px 16px;'
        f'margin:16px 0;border-radius:4px;">\n'
        f"        <strong>Cluster Data Coverage Report</strong>"
        f" (requested: {days_requested} days)<br>\n"
        f'        <em style="font-size:0.9em;color:#555;">{heading}</em>\n'
        f'        <table style="margin-top:8px;border-collapse:collapse;font-size:0.9em;">\n'
        f'          <tr style="font-weight:bold;">'
        f'<td style="padding:4px 10px;">Cluster</td>'
        f'<td style="padding:4px 10px;">Coverage</td>'
        f'<td style="padding:4px 10px;">Oldest Data Point</td>'
        f'<td style="padding:4px 10px;">Pod Executions</td></tr>\n'
        f"{rows}"
        f"        </table>\n"
        f"    </div>\n"
    )


# ---------------------------------------------------------------------------
# Finding 3: Scrape-interval transparency and heavy-tail warnings
# ---------------------------------------------------------------------------


def _html_scrape_interval_note():
    """HTML advisory about Prometheus scrape interval and sub-scrape memory spikes."""
    return (
        '    <div style="background:#e8f4fd;border:1px solid #90caf9;padding:12px 16px;'
        'margin:16px 0;border-radius:4px;">\n'
        "        <strong>&#8505; Prometheus Scrape Interval Notice</strong>\n"
        '        <p style="margin:8px 0 0 0;'
        'font-size:0.95em;color:#333;">'
        "Memory values are the <em>maximum of all scraped"
        " samples</em> within the analysis window. "
        "The Prometheus scrape interval for "
        "<code>container_memory_working_set_bytes</code>"
        " is typically <strong>15&ndash;30 seconds"
        "</strong> on Konflux clusters. A transient "
        "memory spike that resolves faster than one "
        "scrape interval (e.g. a brief image-"
        "decompression burst) will <strong>not</strong>"
        " appear in the data. Steps that perform large,"
        " short-lived memory allocations (image pulls, "
        "decompression, GC) may have actual peak usage "
        "higher than reported. For long-term "
        "improvement, Splunk-archived metrics at higher "
        "resolution can be used to cross-check "
        "Prometheus data.</p>\n"
        "    </div>\n"
    )


def compute_heavy_tail_warnings(all_recommendations_by_base):
    """Return per-step heavy-tail warnings when max/p95 ratio exceeds threshold.

    Returns a list of dicts:
        [{'step': str, 'mem_max_mb': float, 'mem_p95_mb': float, 'ratio': float,
          'p99_mb': float (if available), 'cpu_max': float, 'cpu_p95': float}]
    """
    RATIO_WARN = 3.0
    warnings = []
    max_recs = all_recommendations_by_base.get("max", [])
    p95_recs = all_recommendations_by_base.get("p95", [])

    p95_by_step = {r["step_name"]: r for r in p95_recs if r}
    for rec in max_recs:
        if not rec:
            continue
        step = rec["step_name"]
        mem_max = rec.get("mem_max_max", 0)
        p95_rec = p95_by_step.get(step)
        mem_p95 = p95_rec.get("mem_p95_max", 0) if p95_rec else rec.get("mem_p95_max", 0)
        cpu_max = rec.get("cpu_max_max", 0)
        cpu_p95 = p95_rec.get("cpu_p95_max", 0) if p95_rec else rec.get("cpu_p95_max", 0)
        if mem_p95 > 0 and mem_max / mem_p95 >= RATIO_WARN:
            warnings.append(
                {
                    "step": step,
                    "mem_max_mb": mem_max,
                    "mem_p95_mb": mem_p95,
                    "ratio": round(mem_max / mem_p95, 1),
                    "cpu_max_cores": cpu_max,
                    "cpu_p95_cores": cpu_p95,
                }
            )
    return warnings


def _html_heavy_tail_warnings_banner(warnings):
    """HTML banner surfacing heavy-tail distribution warnings for reviewers."""
    if not warnings:
        return ""

    rows = ""
    for w in warnings:
        mem_max_k8s = mb_to_kubernetes(w["mem_max_mb"])
        mem_p95_k8s = mb_to_kubernetes(w["mem_p95_mb"])
        rows += (
            f"<tr>"
            f'<td style="padding:5px 10px;font-weight:bold;">'
            f"{html.escape(normalize_step_name_for_compare(w['step']))}"
            f"</td>"
            f'<td style="padding:5px 10px;">{mem_max_k8s} ({w["mem_max_mb"]:.0f} MB)</td>'
            f'<td style="padding:5px 10px;">{mem_p95_k8s} ({w["mem_p95_mb"]:.0f} MB)</td>'
            f'<td style="padding:5px 10px;color:#c62828;font-weight:bold;">{w["ratio"]}&#215;</td>'
            f"</tr>\n"
        )

    return (
        '    <div style="background:#fff8e1;border:2px solid #f9a825;padding:12px 16px;'
        'margin:16px 0;border-radius:4px;">\n'
        "        <strong>&#9888; Heavy-Tail Distribution Warning</strong>\n"
        '        <p style="margin:8px 0 4px 0;font-size:0.95em;color:#555;">'
        "The following step(s) show a Max/P95 memory ratio &ge; 3&times;. "
        "This means rare but heavy workloads (outlier tenants, large bundles) can require "
        "significantly more memory than the p95 recommendation covers. "
        "Consider using the <strong>MAX</strong> base or a per-tenant resource override "
        "for these steps if OOM failures occur on outlier workloads.</p>\n"
        '        <table style="border-collapse:collapse;font-size:0.9em;margin-top:8px;">\n'
        '          <tr style="font-weight:bold;background:#fef9c3;">'
        '<td style="padding:5px 10px;">Step</td>'
        '<td style="padding:5px 10px;">Max Observed</td>'
        '<td style="padding:5px 10px;">P95</td>'
        '<td style="padding:5px 10px;">Max/P95 Ratio</td></tr>\n'
        f"{rows}"
        "        </table>\n"
        "    </div>\n"
    )


def _compute_violators_for_step(detailed_executions, step_name, mem_base, cpu_base):
    """Find pod executions that exceed a given memory or CPU base threshold for one step.

    Groups results as: namespace → application → list-of-component-dicts.

    Returns:
        dict  { namespace: { application: [ {component, cluster, mem_max, cpu_max,
                                              count, mem_viol, cpu_viol} ] } }
        or empty dict if there are no violations.
    """
    from collections import defaultdict as _dd

    # Only violations make sense when there is a non-zero base
    has_mem_base = mem_base and mem_base > 0
    has_cpu_base = cpu_base and cpu_base > 0
    if not has_mem_base and not has_cpu_base:
        return {}

    # Normalise the step name we look for (executions may carry it without 'step-' prefix)
    step_bare = step_name.removeprefix("step-") if step_name.startswith("step-") else step_name

    groups = _dd(lambda: {"mem_vals": [], "cpu_vals": []})
    for r in detailed_executions:
        r_step = r.get("step", "")
        r_step_bare = r_step.removeprefix("step-") if r_step.startswith("step-") else r_step
        if r_step_bare != step_bare:
            continue
        mem = float(r.get("memory_mb", 0) or 0)
        cpu = float(r.get("cpu_cores", 0) or 0)
        mem_viol = has_mem_base and mem > mem_base
        cpu_viol = has_cpu_base and cpu > cpu_base
        if not (mem_viol or cpu_viol):
            continue
        key = (
            r.get("namespace", "unknown"),
            r.get("application", "unknown"),
            r.get("component", "unknown"),
            r.get("cluster", "unknown"),
        )
        groups[key]["mem_vals"].append(mem)
        groups[key]["cpu_vals"].append(cpu)

    if not groups:
        return {}

    result = _dd(lambda: _dd(list))
    for (ns, app, comp, cluster), vals in groups.items():
        mem_max = max(vals["mem_vals"]) if vals["mem_vals"] else 0
        cpu_max = max(vals["cpu_vals"]) if vals["cpu_vals"] else 0
        result[ns][app].append(
            {
                "component": comp,
                "cluster": cluster,
                "mem_max": mem_max,
                "cpu_max": cpu_max,
                "count": len(vals["mem_vals"]),
                "mem_viol": has_mem_base and mem_max > mem_base,
                "cpu_viol": has_cpu_base and cpu_max > cpu_base,
            }
        )

    # Sort components within each app by descending peak memory
    for ns in result:
        for app in result[ns]:
            result[ns][app].sort(key=lambda x: -x["mem_max"])

    return result


def _html_violators_block(
    viol_by_ns, mem_base, cpu_base, base_label, step_display_name, _table_counter=None
):
    """Render a sortable, collapsible HTML block listing executions that exceed a base threshold.

    Produces a flat table (no rowspan) with data-val attributes on the numeric columns so
    that the sortVT() JavaScript function can sort by Peak Mem, vs Base, Peak CPU, # Pods.
    Namespace and Application cells keep their coloured backgrounds for visual grouping.
    """
    if _table_counter is None:
        _table_counter = [0]
    if not viol_by_ns:
        return (
            f'<p class="no-violators">&#10003;&nbsp; No executions exceed the '
            f"<em>{base_label}</em> baseline for step "
            f"<strong>{html.escape(step_display_name)}</strong>.</p>\n"
        )

    def _fmt_mb(mb):
        return f"{mb / 1024:.2f} Gi" if mb >= 1024 else f"{mb:.0f} Mi"

    def _fmt_cpu(c):
        if c == 0:
            return "0m"
        return f"{int(c * 1000)}m" if c < 1 else f"{c:.3f}"

    mem_thr = _fmt_mb(mem_base) if (mem_base and mem_base > 0) else "—"
    cpu_thr = _fmt_cpu(cpu_base) if (cpu_base and cpu_base > 0) else "—"

    # Flatten nested dict → list of row dicts, sorted by peak mem desc
    flat_rows = []
    for ns in sorted(viol_by_ns):
        for app in sorted(viol_by_ns[ns]):
            for row in viol_by_ns[ns][app]:
                flat_rows.append(dict(row, namespace=ns, application=app))
    flat_rows.sort(key=lambda r: -r["mem_max"])

    _table_counter[0] += 1
    tid = f"viol_tbl_{_table_counter[0]}"

    rows_html = ""
    for row in flat_rows:
        mem_over = row["mem_max"] - mem_base if row["mem_viol"] else 0
        cpu_over = row["cpu_max"] - cpu_base if row["cpu_viol"] else 0
        mem_vs = f"+{_fmt_mb(mem_over)}&nbsp;&#9888;" if row["mem_viol"] else "&#10003;&nbsp;ok"
        cpu_vs = f"+{_fmt_cpu(cpu_over)}&nbsp;&#9888;" if row["cpu_viol"] else "&#10003;&nbsp;ok"
        mc = ' class="viol-cell"' if row["mem_viol"] else ""
        cc = ' class="viol-cell"' if row["cpu_viol"] else ""
        # data-val carries raw numeric value for JS sort:
        #   mem / cpu → MB or millicores; "ok" rows get -1 so they sort to bottom
        mem_dv = f"{row['mem_max']:.1f}"
        memov_dv = f"{mem_over:.1f}" if row["mem_viol"] else "-1"
        cpu_dv = f"{row['cpu_max'] * 1000:.1f}"
        cpuov_dv = f"{cpu_over * 1000:.1f}" if row["cpu_viol"] else "-1"
        rows_html += (
            f"<tr>"
            f'<td class="viol-ns-cell">{html.escape(row["namespace"])}</td>'
            f'<td class="viol-app-cell">{html.escape(row["application"])}</td>'
            f"<td>{html.escape(row['component'])}</td>"
            f'<td><span class="cluster-badge">{html.escape(row["cluster"])}</span></td>'
            f'<td{mc} data-val="{mem_dv}">{_fmt_mb(row["mem_max"])}</td>'
            f'<td{mc} data-val="{memov_dv}">{mem_vs}</td>'
            f'<td{cc} data-val="{cpu_dv}">{_fmt_cpu(row["cpu_max"])}</td>'
            f'<td{cc} data-val="{cpuov_dv}">{cpu_vs}</td>'
            f'<td data-val="{row["count"]}">{row["count"]}</td>'
            f"</tr>\n"
        )

    return f"""<details class="violators-block">
  <summary class="violators-summary">
    &#9888;&nbsp; <strong>{len(flat_rows)}&nbsp;group(s)</strong>
    exceed the <em>{base_label}</em> baseline
    ({mem_thr}&nbsp;mem&nbsp;/&nbsp;{cpu_thr}&nbsp;cpu) for step
    <strong>{html.escape(step_display_name)}</strong>&nbsp;&#9660;
  </summary>
  <div class="violators-body">
    <p class="sort-hint">Click amber column header to sort &nbsp;&#8597;</p>
    <table id="{tid}" class="violators-table">
      <thead><tr>
        <th>Namespace</th><th>Application</th><th>Component</th><th>Cluster</th>
        <th class="sortable" onclick="sortVT('{tid}',4)">Peak&nbsp;Mem</th>
        <th class="sortable" onclick="sortVT('{tid}',5)">vs&nbsp;Base&nbsp;(Mem)</th>
        <th class="sortable" onclick="sortVT('{tid}',6)">Peak&nbsp;CPU</th>
        <th class="sortable" onclick="sortVT('{tid}',7)">vs&nbsp;Base&nbsp;(CPU)</th>
        <th class="sortable" onclick="sortVT('{tid}',8)">#&nbsp;Pods</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</details>
"""


def read_wrapper_config(wrapper_path):
    """Read TASK_NAME and STEPS from wrapper script.

    Returns:
        tuple: (task_name, steps_list, is_defined)
        - task_name: Task name if defined, None otherwise
        - steps_list: List of step names (without 'step-' prefix) if defined, None otherwise
        - is_defined: True if both TASK_NAME and STEPS are defined (not commented, non-empty)
    """
    task_name = None
    steps_list = None
    is_defined = False

    try:
        with open(wrapper_path) as f:
            lines = f.readlines()

        for line in lines:
            stripped = line.strip()
            # Skip comments and empty lines
            if not stripped or stripped.startswith("#"):
                continue

            # Check for TASK_NAME
            if stripped.startswith("TASK_NAME="):
                # Extract value between quotes
                match = re.search(r'TASK_NAME="([^"]*)"', line)
                if match:
                    task_name = match.group(1).strip()

            # Check for STEPS
            elif stripped.startswith("STEPS="):
                # Extract value between quotes
                match = re.search(r'STEPS="([^"]*)"', line)
                if match:
                    steps_str = match.group(1).strip()
                    if steps_str:
                        # Split by space and remove 'step-' prefix
                        steps_list = [normalize_step_name_for_compare(s) for s in steps_str.split()]

        # Both must be defined and non-empty
        is_defined = (
            task_name is not None
            and task_name != ""
            and steps_list is not None
            and len(steps_list) > 0
        )

    except Exception as e:
        print(f"Warning: Could not read wrapper script: {e}", file=sys.stderr)

    return task_name, steps_list, is_defined


def validate_wrapper_steps(wrapper_task, wrapper_steps, yaml_task, yaml_steps):
    """Validate wrapper-defined task and steps against YAML file.

    Args:
        wrapper_task: Task name from wrapper script
        wrapper_steps: List of step names from wrapper (without 'step-' prefix)
        yaml_task: Task name from YAML file
        yaml_steps: List of step names from YAML file

    Returns:
        tuple: (is_valid, error_messages)
        - is_valid: True if validation passes
        - error_messages: List of error messages (empty if valid)
    """
    errors = []

    # Check task name match (case-sensitive)
    if wrapper_task != yaml_task:
        errors.append(
            f"Task name mismatch: wrapper has '{wrapper_task}', YAML file has '{yaml_task}'"
        )

    # Convert to sets for comparison (normalize step names)
    wrapper_steps_set = set(wrapper_steps)
    yaml_steps_set = set(yaml_steps)

    # Check if wrapper steps are subset or equal to YAML steps
    extra_steps = wrapper_steps_set - yaml_steps_set
    if extra_steps:
        errors.append(f"Wrapper defines steps not found in YAML file: {sorted(extra_steps)}")

    missing_steps = yaml_steps_set - wrapper_steps_set
    if missing_steps:
        # This is a warning, not an error (wrapper can be a subset)
        pass

    is_valid = len(errors) == 0
    return is_valid, errors


def check_cluster_connectivity(wrapper_path):
    """Check connectivity to all clusters defined in wrapper script.

    Returns:
        tuple: (all_connected, connectivity_report)
        - all_connected: True if all clusters are accessible
        - connectivity_report: List of (cluster_display_name, status, error_message) tuples
                              Note: cluster_display_name is the short name for display purposes
    """
    report = []
    all_connected = True

    try:
        with open(wrapper_path) as f:
            lines = f.readlines()

        # Extract CONTEXTS line (only non-commented lines, similar to read_wrapper_config)
        contexts_str = None
        for line in lines:
            stripped = line.strip()
            # Skip comments and empty lines
            if not stripped or stripped.startswith("#"):
                continue
            # Check for CONTEXTS (not commented)
            if stripped.startswith("CONTEXTS="):
                # Extract value between quotes
                match = re.search(r'CONTEXTS="([^"]*)"', line)
                if match:
                    contexts_str = match.group(1).strip()
                    break

        if not contexts_str:
            # No CONTEXTS found in non-commented lines, try to get contexts from kubectl as fallback
            result = subprocess.run(
                ["kubectl", "config", "get-contexts", "-o", "name"],
                capture_output=True,
                text=True,
                timeout=120,  # 2 minutes timeout for connectivity check
            )
            if result.returncode == 0:
                contexts = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
            else:
                return False, [("unknown", False, "Could not get cluster contexts")]
        else:
            # Handle cases where CONTEXTS uses command substitution
            if "$(" in contexts_str:
                # Execute command substitution to get contexts
                # Extract the command inside $()
                cmd_match = re.search(r"\$\(([^)]+)\)", contexts_str)
                if cmd_match:
                    cmd = cmd_match.group(1).strip()
                    # Handle the specific case: kubectl config get-contexts -o name 2>/dev/null |
                    # xargs
                    if "kubectl config get-contexts" in cmd:
                        result = subprocess.run(
                            ["kubectl", "config", "get-contexts", "-o", "name"],
                            capture_output=True,
                            text=True,
                            timeout=120,  # 2 minutes timeout for connectivity check
                        )
                        if result.returncode == 0:
                            contexts = [
                                c.strip() for c in result.stdout.strip().split("\n") if c.strip()
                            ]
                        else:
                            # Fallback to default if command fails (extract from echo)
                            fallback_match = re.search(r'echo\s+[\'"]([^\'"]+)[\'"]', contexts_str)
                            if fallback_match:
                                contexts = [fallback_match.group(1).strip()]
                            else:
                                return False, [
                                    ("unknown", False, "Could not execute CONTEXTS command")
                                ]
                    else:
                        return False, [("unknown", False, f"Unsupported CONTEXTS command: {cmd}")]
                else:
                    return False, [("unknown", False, "Invalid CONTEXTS command substitution")]
            else:
                # Simple string value - split by space
                contexts = [c.strip() for c in contexts_str.split() if c.strip()]

        # Test connectivity to each cluster
        for ctx in contexts:
            if not ctx:
                continue
            try:
                result = subprocess.run(
                    ["kubectl", "config", "use-context", ctx],
                    capture_output=True,
                    text=True,
                    timeout=120,  # 2 minutes timeout for connectivity check
                )
                if result.returncode == 0:
                    # Try a simple kubectl command to verify connectivity
                    test_result = subprocess.run(
                        ["kubectl", "get", "namespaces", "--request-timeout=2m"],
                        capture_output=True,
                        text=True,
                        timeout=120,  # 2 minutes timeout for connectivity check
                    )
                    if test_result.returncode == 0:
                        # Use display name for report, but ctx (full context) is still used for
                        # operations
                        display_name = get_cluster_display_name(ctx)
                        report.append((display_name, True, "Connected"))
                    else:
                        display_name = get_cluster_display_name(ctx)
                        report.append(
                            (
                                display_name,
                                False,
                                f"Cannot access cluster: {test_result.stderr[:100]}",
                            )
                        )
                        all_connected = False
                else:
                    display_name = get_cluster_display_name(ctx)
                    report.append(
                        (display_name, False, f"Cannot switch context: {result.stderr[:100]}")
                    )
                    all_connected = False
            except subprocess.TimeoutExpired:
                display_name = get_cluster_display_name(ctx)
                report.append((display_name, False, "Connection timeout"))
                all_connected = False
            except Exception as e:
                display_name = get_cluster_display_name(ctx)
                report.append((display_name, False, f"Error: {str(e)[:100]}"))
                all_connected = False

    except Exception as e:
        return False, [("unknown", False, f"Error reading wrapper script: {str(e)}")]

    return all_connected, report


def prompt_confirmation(task_name, steps, source="extracted from YAML"):
    """Prompt user for confirmation before proceeding.

    Args:
        task_name: Task name to confirm
        steps: List of step names to confirm
        source: Source of the values (for display)

    Returns:
        bool: True if user confirms, False otherwise
    """
    print("\n" + "=" * 80, file=sys.stderr)
    print("CONFIRMATION: Task and Steps Configuration", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"Source: {source}", file=sys.stderr)
    print(f"Task Name: {task_name}", file=sys.stderr)
    print(f"Steps ({len(steps)}):", file=sys.stderr)
    for i, step in enumerate(steps, 1):
        print(f"  {i}. {step}", file=sys.stderr)
    print("=" * 80, file=sys.stderr)

    while True:
        # CRITICAL: Flush stderr before prompting to ensure any previous output is visible
        sys.stderr.flush()
        sys.stdout.flush()
        response = input("Proceed with these values? [y/N]: ").strip().lower()
        if response in ("y", "yes"):
            # Print newline after confirmation to ensure clean separation
            print("", file=sys.stderr)
            sys.stderr.flush()
            return True
        elif response in ("n", "no", ""):
            return False
        else:
            print("Please enter 'y' or 'n'", file=sys.stderr)


def extract_cluster_list(wrapper_path):
    """Extract list of clusters from wrapper script.

    Returns:
        list: List of cluster context names
    """
    try:
        with open(wrapper_path) as f:
            lines = f.readlines()

        # Extract CONTEXTS line (only non-commented lines, same logic as check_cluster_connectivity)
        contexts_str = None
        for line in lines:
            stripped = line.strip()
            # Skip comments and empty lines
            if not stripped or stripped.startswith("#"):
                continue
            # Check for CONTEXTS (not commented)
            if stripped.startswith("CONTEXTS="):
                # Extract value between quotes
                match = re.search(r'CONTEXTS="([^"]*)"', line)
                if match:
                    contexts_str = match.group(1).strip()
                    break

        if not contexts_str:
            # Try to get contexts from kubectl
            result = subprocess.run(
                ["kubectl", "config", "get-contexts", "-o", "name"],
                capture_output=True,
                text=True,
                timeout=120,  # 2 minutes timeout for connectivity check
            )
            if result.returncode == 0:
                contexts = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
            else:
                return []
        else:
            # Handle cases where CONTEXTS uses command substitution
            if "$(" in contexts_str:
                # Execute command substitution
                cmd_match = re.search(r"\$\(([^)]+)\)", contexts_str)
                if cmd_match:
                    cmd = cmd_match.group(1).strip()
                    if "kubectl config get-contexts" in cmd:
                        result = subprocess.run(
                            ["kubectl", "config", "get-contexts", "-o", "name"],
                            capture_output=True,
                            text=True,
                            timeout=120,  # 2 minutes timeout for connectivity check
                        )
                        if result.returncode == 0:
                            contexts = [
                                c.strip() for c in result.stdout.strip().split("\n") if c.strip()
                            ]
                        else:
                            # Fallback to default if command fails
                            fallback_match = re.search(r'echo\s+[\'"]([^\'"]+)[\'"]', contexts_str)
                            if fallback_match:
                                contexts = [fallback_match.group(1).strip()]
                            else:
                                return []
                    else:
                        return []
                else:
                    return []
            else:
                # Simple string value - split by space
                contexts = [c.strip() for c in contexts_str.split() if c.strip()]

        # Remove duplicates while preserving order
        # CRITICAL: Use dict.fromkeys() for guaranteed deduplication
        # This is more efficient and ensures no duplicates slip through
        unique_contexts = list(
            dict.fromkeys(ctx.strip() for ctx in contexts if ctx and ctx.strip())
        )

        return unique_contexts
    except Exception as e:
        print(f"Warning: Could not extract cluster list: {e}", file=sys.stderr)
        return []


def get_cluster_display_name(cluster_ctx):
    """Extract short cluster display name from full context string.

    This function extracts a user-friendly short name for display purposes only.
    The full context string should still be used for all cluster operations.

    Example:
        Input:  'default/api-stone-prd-rh01-pg1f-p1-openshiftapps-com:6443/smodak'
        Output: 'stone-prd-rh01'

    Uses same regex logic as wrapper_for_promql.sh: s#.*/api-([^-]+-[^-]+-[^-]+).*#\1#

    Args:
        cluster_ctx: Full cluster context string (e.g., 'default/api-stone-prd-rh01-...')

    Returns:
        str: Short cluster display name (e.g., 'stone-prd-rh01')
    """
    # Use same regex as wrapper_for_promql.sh: s#.*/api-([^-]+-[^-]+-[^-]+).*#\1#
    match = re.search(r"/api-([^-]+-[^-]+-[^-]+)", cluster_ctx)
    if match:
        return match.group(1)
    # Fallback: try to extract from context name
    if "/" in cluster_ctx:
        parts = cluster_ctx.split("/")
        if len(parts) > 1:
            # Try to extract from parts
            for part in parts:
                if "api-" in part:
                    match = re.search(r"api-([^-]+-[^-]+-[^-]+)", part)
                    if match:
                        return match.group(1)
    # Last resort: return last part after /
    return cluster_ctx.split("/")[-1] if "/" in cluster_ctx else cluster_ctx


def _spinner_thread(stop_event, progress_data=None, progress_lock=None, total_clusters=0):
    """Display a spinning wheel with percentage progress while collecting data from clusters."""
    spinner_chars = ["|", "/", "-", "\\"]
    idx = 0
    while not stop_event.is_set():
        percentage = 0
        pods_listed = 0
        pods_queried = 0
        pods_kept = 0
        if progress_data and total_clusters > 0 and progress_lock:
            with progress_lock:
                completed_count = len(progress_data.get("completed", []))
                pods_listed = progress_data.get("pods_listed", 0)
                pods_queried = progress_data.get("pods_queried", 0)
                pods_kept = progress_data.get("pods_kept", 0)
            percentage = int((completed_count / total_clusters) * 100)
        spin = spinner_chars[idx % len(spinner_chars)]
        if percentage > 0:
            message = (
                f"Getting information from all clusters: "
                f"(Completed No. of clusters: {percentage}%) "
                f"[listed={pods_listed} queried={pods_queried} kept={pods_kept}] {spin}"
            )
        else:
            message = (
                f"Getting information from all clusters: "
                f"[listed={pods_listed} queried={pods_queried} kept={pods_kept}] {spin}"
            )
        print(f"\r{message}", end="", file=sys.stderr)
        sys.stderr.flush()
        idx += 1
        time.sleep(0.2)
    print("\r\033[K", end="", file=sys.stderr)
    sys.stderr.flush()


def format_promql_duration(seconds):
    """Format a lookback window for PromQL range selectors (e.g. 1d, 6h, 90m)."""
    seconds = int(seconds)
    if seconds <= 0:
        return "0s"
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def format_lookback_label(days, hours):
    """Human-readable lookback like '7d', '6h', or '1d+6h'."""
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    return "+".join(parts) if parts else "0"


def resolve_lookback_seconds(days, hours):
    """Combine --days and --hours into a total lookback in seconds."""
    days = int(days or 0)
    hours = int(hours or 0)
    if days < 0 or hours < 0:
        raise ValueError("--days and --hours must be >= 0")
    total = days * 86400 + hours * 3600
    if total <= 0:
        raise ValueError("Lookback window must be > 0 (use --days and/or --hours)")
    return total


def _empty_collection_counters():
    return {
        "pods_listed": 0,
        "pods_queried": 0,
        "pods_kept": 0,
        "query_failures": 0,
        "empty_metrics": 0,
        "parse_errors": 0,
        "list_failures": 0,
    }


def _merge_counters(dest, src):
    for key, value in src.items():
        dest[key] = dest.get(key, 0) + value


DEBUG_SKIP_SAMPLE_LIMIT = 15


def parse_csv_data(csv_text):
    """Parse CSV data (same format as wrapper script or detailed_executions_to_csv output)."""
    data = []
    lines = [line for line in csv_text.strip().split("\n") if line.strip()]
    if not lines:
        return data

    # Check if we only have header line (no data rows)
    if len(lines) <= 1:
        return data

    reader = csv.DictReader(lines)
    for row in reader:
        # Clean up keys (remove spaces and quotes)
        cleaned_row = {k.strip().strip('"'): v.strip().strip('"') for k, v in row.items()}
        data.append(cleaned_row)
    return data


def round_memory_to_standard(mb):
    """Round memory to standard Kubernetes values.

    For values < 1Gi: round to increments of 64Mi (64Mi, 128Mi, 192Mi, 256Mi, etc.)
    For values >= 1Gi: round to whole Gi values (1Gi, 2Gi, 3Gi, etc.)

    Minimum value: 64Mi (allows fine granularity while being more standard than 32Mi)
    Always rounds up to ensure we don't go below the recommended value.
    """
    mb = float(mb)

    # Enforce minimum of 64Mi (allows fine granularity while being more standard than 32Mi)
    MIN_MEMORY_MB = 64

    if mb < MIN_MEMORY_MB:
        mb = MIN_MEMORY_MB

    if mb < 1024:
        # Round UP to next highest increment of 64Mi
        # Using +63 ensures we always round up (e.g., 65Mi -> 128Mi, not 64Mi)
        rounded = ((int(mb) + 63) // 64) * 64

        # Ensure minimum
        if rounded < MIN_MEMORY_MB:
            rounded = MIN_MEMORY_MB

        return rounded
    else:
        # Round to nearest whole Gi, but round up if fractional
        gi = mb / 1024.0
        rounded_gi = round(gi)
        # If rounding down would go below original, round up instead
        if rounded_gi * 1024 < mb:
            rounded_gi += 1
        return rounded_gi * 1024


def mb_to_kubernetes(mb):
    """Convert MB to Kubernetes memory format with standard rounding."""
    mb = float(mb)
    rounded_mb = round_memory_to_standard(mb)

    if rounded_mb < 1024:
        return f"{int(rounded_mb)}Mi"
    else:
        gi = rounded_mb / 1024.0
        # Should always be whole number after rounding
        return f"{int(gi)}Gi"


def round_cpu_to_standard(cores):
    """Round CPU to standard Kubernetes values.

    Rounds UP to next highest increment of 50m (50m, 100m, 150m, 200m, etc.)
    Minimum value: 50m (allows finer granularity)
    Always rounds up to ensure we don't go below the recommended value.
    """
    cores = float(cores)
    millicores = cores * 1000

    # Enforce minimum of 50m (allows finer granularity)
    MIN_CPU_MILLICORES = 50

    if millicores < MIN_CPU_MILLICORES:
        millicores = MIN_CPU_MILLICORES

    # Round UP to next highest increment of 50m
    # Using +49 ensures we always round up (e.g., 51m -> 100m, not 50m)
    rounded_m = ((int(millicores) + 49) // 50) * 50

    # Ensure minimum
    if rounded_m < MIN_CPU_MILLICORES:
        rounded_m = MIN_CPU_MILLICORES

    return rounded_m / 1000.0


def cores_to_kubernetes(cores):
    """Convert cores to Kubernetes CPU format, always in millicores."""
    cores = float(cores)
    rounded_cores = round_cpu_to_standard(cores)

    # Always return as millicores
    millicores = int(rounded_cores * 1000)
    return f"{millicores}m"


def parse_cpu_value(cpu_str):
    """Parse CPU value from format like '3569m' or '4.5'."""
    if not cpu_str or cpu_str == "0m" or cpu_str == "0":
        return 0.0
    if cpu_str.endswith("m"):
        return float(cpu_str[:-1]) / 1000.0
    return float(cpu_str)


def _percentile(sorted_values, p):
    """Return the value at percentile p (0..1) from a sorted list. Empty -> 0."""
    if not sorted_values:
        return 0
    idx = int((len(sorted_values) - 1) * p)
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


def detailed_executions_to_csv(executions):
    """Build main pipeline CSV from detailed per-pod executions (single source of truth).

    Groups by (cluster, task, step); computes max, p95, p90, median for memory and CPU;
    outputs one row per group in the same format as the wrapper CSV for parse_csv_data.
    Step names in output are without 'step-' prefix (e.g. prefetch-dependencies).

    Returns:
        str: CSV string with header and data rows, or empty string if no executions.
    """
    if not executions:
        return ""
    header = (
        '"cluster", "task", "step", "pod_max_mem", '
        '"namespace_max_mem", "component_max_mem", "application_max_mem", '
        '"mem_max_mb", "mem_p95_mb", "mem_p90_mb", "mem_median_mb", '
        '"pod_max_cpu", "namespace_max_cpu", "component_max_cpu", "application_max_cpu", '
        '"cpu_max", "cpu_p95", "cpu_p90", "cpu_median"'
    )
    by_key = defaultdict(list)
    for e in executions:
        cluster = e.get("cluster", "")
        task = e.get("task", "")
        step = e.get("step", "")  # already without step- prefix
        if not step:
            continue
        key = (cluster, task, step)
        by_key[key].append(e)
    rows = []
    for cluster, task, step in sorted(by_key.keys()):
        group = by_key[(cluster, task, step)]
        mem_vals = sorted([float(x.get("memory_mb", 0)) for x in group])
        cpu_vals = sorted([float(x.get("cpu_cores", 0)) for x in group])
        mem_max = max(mem_vals) if mem_vals else 0
        mem_p95 = _percentile(mem_vals, 0.95)
        mem_p90 = _percentile(mem_vals, 0.90)
        mem_median = _percentile(mem_vals, 0.50)
        cpu_max = max(cpu_vals) if cpu_vals else 0
        cpu_p95 = _percentile(cpu_vals, 0.95)
        cpu_p90 = _percentile(cpu_vals, 0.90)
        cpu_median = _percentile(cpu_vals, 0.50)
        # Pod/namespace/component/application for max mem
        max_mem_exec = max(group, key=lambda x: float(x.get("memory_mb", 0)))
        pod_max_mem = max_mem_exec.get("pod", "")
        namespace_max_mem = max_mem_exec.get("namespace", "")
        component_max_mem = max_mem_exec.get("component", "N/A")
        application_max_mem = max_mem_exec.get("application", "N/A")
        # Pod/namespace/component/application for max cpu
        max_cpu_exec = max(group, key=lambda x: float(x.get("cpu_cores", 0)))
        pod_max_cpu = max_cpu_exec.get("pod", "")
        namespace_max_cpu = max_cpu_exec.get("namespace", "")
        component_max_cpu = max_cpu_exec.get("component", "N/A")
        application_max_cpu = max_cpu_exec.get("application", "N/A")
        cpu_max_m = f"{int(round(cpu_max * 1000))}m"
        cpu_p95_m = f"{int(round(cpu_p95 * 1000))}m"
        cpu_p90_m = f"{int(round(cpu_p90 * 1000))}m"
        cpu_median_m = f"{int(round(cpu_median * 1000))}m"
        row = (
            f'"{cluster}", "{task}", "{step}", '
            f'"{pod_max_mem}", "{namespace_max_mem}", '
            f'"{component_max_mem}", "{application_max_mem}", '
            f'"{int(round(mem_max))}", "{int(round(mem_p95))}", '
            f'"{int(round(mem_p90))}", "{int(round(mem_median))}", '
            f'"{pod_max_cpu}", "{namespace_max_cpu}", '
            f'"{component_max_cpu}", "{application_max_cpu}", '
            f'"{cpu_max_m}", "{cpu_p95_m}", "{cpu_p90_m}", "{cpu_median_m}"'
        )
        rows.append(row)
    return header + "\n" + "\n".join(rows)


def _parse_cpu_millicores_for_verify(s):
    """Parse CPU from main CSV e.g. '1194m' -> 1194."""
    s = (s or "").strip().rstrip("m")
    if not s:
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def verify_aggregates_against_detailed(detailed_executions, aggregated_rows):
    """Verify aggregated rows match recomputation from detailed executions.

    Used for single-source sanity check: the main table is derived
    from the same executions as the detailed files; this recomputes
    per (cluster, step) from executions and compares.

    Args:
        detailed_executions: List of execution dicts
            (cluster, step, memory_mb, cpu_cores, ...)
        aggregated_rows: List of parsed CSV rows (from parse_csv_data)
            with mem_max_mb, mem_p95_mb, etc.

    Returns:
        tuple: (all_ok: bool, messages: list of str)
    """
    if not detailed_executions or not aggregated_rows:
        return True, []
    by_key = defaultdict(list)
    for e in detailed_executions:
        cluster = e.get("cluster", "")
        step = e.get("step", "")
        if not step:
            continue
        key = (cluster, step)
        by_key[key].append(e)
    main_by_key = {}
    for row in aggregated_rows:
        cluster = (row.get("cluster") or "").strip()
        step = (row.get("step") or "").strip()
        if not cluster or not step:
            continue
        key = (cluster, step)
        main_by_key[key] = {
            "mem_max_mb": int(float(row.get("mem_max_mb") or 0)),
            "mem_p95_mb": int(float(row.get("mem_p95_mb") or 0)),
            "mem_p90_mb": int(float(row.get("mem_p90_mb") or 0)),
            "mem_median_mb": int(float(row.get("mem_median_mb") or 0)),
            "cpu_max_m": _parse_cpu_millicores_for_verify(row.get("cpu_max")),
            "cpu_p95_m": _parse_cpu_millicores_for_verify(row.get("cpu_p95")),
            "cpu_p90_m": _parse_cpu_millicores_for_verify(row.get("cpu_p90")),
            "cpu_median_m": _parse_cpu_millicores_for_verify(row.get("cpu_median")),
        }
    messages = []
    all_ok = True
    for key in sorted(by_key.keys()):
        cluster, step = key
        group = by_key[key]
        mem_vals = sorted([float(x.get("memory_mb", 0)) for x in group])
        cpu_vals = sorted([float(x.get("cpu_cores", 0)) for x in group])
        comp = {
            "mem_max_mb": int(round(max(mem_vals))) if mem_vals else 0,
            "mem_p95_mb": int(round(_percentile(mem_vals, 0.95))),
            "mem_p90_mb": int(round(_percentile(mem_vals, 0.90))),
            "mem_median_mb": int(round(_percentile(mem_vals, 0.50))),
            "cpu_max_m": int(round(max(cpu_vals) * 1000)) if cpu_vals else 0,
            "cpu_p95_m": int(round(_percentile(cpu_vals, 0.95) * 1000)),
            "cpu_p90_m": int(round(_percentile(cpu_vals, 0.90) * 1000)),
            "cpu_median_m": int(round(_percentile(cpu_vals, 0.50) * 1000)),
        }
        main_vals = main_by_key.get(key)
        if not main_vals:
            messages.append(f"Verify: main has no row for cluster={cluster} step={step}")
            all_ok = False
            continue
        mismatches = []
        for k in comp:
            if main_vals[k] != comp[k]:
                mismatches.append(f"{k}: main={main_vals[k]} recomputed={comp[k]}")
        if mismatches:
            all_ok = False
            messages.append(
                f"Verify MISMATCH cluster={cluster} step={step} (n={len(group)} pods): "
                + "; ".join(mismatches)
            )
    return all_ok, messages


def analyze_step_data(step_name, step_rows, margin_pct=10, base="max"):
    """Analyze data for a specific step and return recommendations.

    Args:
        step_name: Name of the step
        step_rows: List of data rows for this step
        margin_pct: Safety margin percentage to add
        base: Base metric to use ('max', 'p95', 'p90', 'median')
    """
    if not step_rows:
        return None

    # Extract memory values
    mem_max_values = [
        float(r["mem_max_mb"])
        for r in step_rows
        if r.get("mem_max_mb") and r["mem_max_mb"] not in ("0", "", "N/A")
    ]
    mem_p95_values = [
        float(r["mem_p95_mb"])
        for r in step_rows
        if r.get("mem_p95_mb") and r["mem_p95_mb"] not in ("0", "", "N/A")
    ]
    mem_p90_values = [
        float(r["mem_p90_mb"])
        for r in step_rows
        if r.get("mem_p90_mb") and r["mem_p90_mb"] not in ("0", "", "N/A")
    ]
    mem_median_values = [
        float(r["mem_median_mb"])
        for r in step_rows
        if r.get("mem_median_mb") and r["mem_median_mb"] not in ("0", "", "N/A")
    ]

    # Extract CPU values
    cpu_max_values = [
        parse_cpu_value(r.get("cpu_max", "0m")) for r in step_rows if r.get("cpu_max")
    ]
    cpu_p95_values = [
        parse_cpu_value(r.get("cpu_p95", "0m")) for r in step_rows if r.get("cpu_p95")
    ]
    cpu_p90_values = [
        parse_cpu_value(r.get("cpu_p90", "0m")) for r in step_rows if r.get("cpu_p90")
    ]
    cpu_median_values = [
        parse_cpu_value(r.get("cpu_median", "0m")) for r in step_rows if r.get("cpu_median")
    ]

    if not mem_max_values:
        return None

    # Calculate max across all clusters
    mem_max_max = max(mem_max_values)
    mem_p95_max = max(mem_p95_values) if mem_p95_values else 0
    mem_p90_max = max(mem_p90_values) if mem_p90_values else 0
    mem_median_max = max(mem_median_values) if mem_median_values else 0

    cpu_max_max = max(cpu_max_values) if cpu_max_values else 0
    cpu_p95_max = max(cpu_p95_values) if cpu_p95_values else 0
    cpu_p90_max = max(cpu_p90_values) if cpu_p90_values else 0
    cpu_median_max = max(cpu_median_values) if cpu_median_values else 0

    # Select base value based on user choice
    if base == "max":
        mem_base = mem_max_max
        cpu_base = cpu_max_max
        base_label = "Max"
    elif base == "p95":
        mem_base = mem_p95_max if mem_p95_max > 0 else mem_max_max
        cpu_base = cpu_p95_max if cpu_p95_max > 0 else cpu_max_max
        base_label = "P95"
    elif base == "p90":
        mem_base = mem_p90_max if mem_p90_max > 0 else mem_max_max
        cpu_base = cpu_p90_max if cpu_p90_max > 0 else cpu_max_max
        base_label = "P90"
    elif base == "median":
        mem_base = mem_median_max if mem_median_max > 0 else mem_max_max
        cpu_base = cpu_median_max if cpu_median_max > 0 else cpu_max_max
        base_label = "Median"
    else:
        # Default to max if invalid option
        mem_base = mem_max_max
        cpu_base = cpu_max_max
        base_label = "Max"

    # Calculate recommendations: base + margin, but don't exceed max observed
    mem_recommended = (
        min(mem_max_max, int(mem_base * (1 + margin_pct / 100))) if mem_base > 0 else mem_max_max
    )
    if cpu_base > 0:
        cpu_recommended = min(cpu_max_max * 1.1, cpu_base * (1 + margin_pct / 100))
    else:
        cpu_recommended = cpu_max_max if cpu_max_max > 0 else 0

    # Count coverage
    mem_coverage = len([x for x in mem_max_values if x <= mem_recommended])
    cpu_coverage = len([x for x in cpu_max_values if x <= cpu_recommended]) if cpu_max_values else 0

    return {
        "step_name": step_name,
        "mem_recommended_mb": mem_recommended,
        "mem_recommended_k8s": mb_to_kubernetes(mem_recommended),
        "cpu_recommended_cores": cpu_recommended,
        "cpu_recommended_k8s": cores_to_kubernetes(cpu_recommended),
        "mem_max_max": mem_max_max,
        "mem_p95_max": mem_p95_max,
        "mem_p90_max": mem_p90_max,
        "mem_median_max": mem_median_max,
        "mem_base": mem_base,
        "cpu_max_max": cpu_max_max,
        "cpu_p95_max": cpu_p95_max,
        "cpu_p90_max": cpu_p90_max,
        "cpu_median_max": cpu_median_max,
        "cpu_base": cpu_base,
        "base_label": base_label,
        "mem_coverage": mem_coverage,
        "mem_total": len(mem_max_values),
        "cpu_coverage": cpu_coverage,
        "cpu_total": len(cpu_max_values) if cpu_max_values else 0,
    }


def analyze_step_data_all_bases(step_name, step_rows, margin_pct=5):
    """Analyze data for a specific step and return recommendations for all base metrics.

    Args:
        step_name: Name of the step
        step_rows: List of data rows for this step
        margin_pct: Safety margin percentage to add (applied to all base metrics)

    Returns:
        Dictionary with recommendations for all base metrics: {'max': {...}, 'p95': {...}, 'p90':
        {...}, 'median': {...}}
    """
    if not step_rows:
        return None

    # Extract memory values
    mem_max_values = [
        float(r["mem_max_mb"])
        for r in step_rows
        if r.get("mem_max_mb") and r["mem_max_mb"] not in ("0", "", "N/A")
    ]
    mem_p95_values = [
        float(r["mem_p95_mb"])
        for r in step_rows
        if r.get("mem_p95_mb") and r["mem_p95_mb"] not in ("0", "", "N/A")
    ]
    mem_p90_values = [
        float(r["mem_p90_mb"])
        for r in step_rows
        if r.get("mem_p90_mb") and r["mem_p90_mb"] not in ("0", "", "N/A")
    ]
    mem_median_values = [
        float(r["mem_median_mb"])
        for r in step_rows
        if r.get("mem_median_mb") and r["mem_median_mb"] not in ("0", "", "N/A")
    ]

    # Extract CPU values
    cpu_max_values = [
        parse_cpu_value(r.get("cpu_max", "0m")) for r in step_rows if r.get("cpu_max")
    ]
    cpu_p95_values = [
        parse_cpu_value(r.get("cpu_p95", "0m")) for r in step_rows if r.get("cpu_p95")
    ]
    cpu_p90_values = [
        parse_cpu_value(r.get("cpu_p90", "0m")) for r in step_rows if r.get("cpu_p90")
    ]
    cpu_median_values = [
        parse_cpu_value(r.get("cpu_median", "0m")) for r in step_rows if r.get("cpu_median")
    ]

    if not mem_max_values:
        return None

    # Calculate max across all clusters
    mem_max_max = max(mem_max_values)
    mem_p95_max = max(mem_p95_values) if mem_p95_values else 0
    mem_p90_max = max(mem_p90_values) if mem_p90_values else 0
    mem_median_max = max(mem_median_values) if mem_median_values else 0

    cpu_max_max = max(cpu_max_values) if cpu_max_values else 0
    cpu_p95_max = max(cpu_p95_values) if cpu_p95_values else 0
    cpu_p90_max = max(cpu_p90_values) if cpu_p90_values else 0
    cpu_median_max = max(cpu_median_values) if cpu_median_values else 0

    # Generate recommendations for all base metrics
    all_recommendations = {}

    for base in ["max", "p95", "p90", "median"]:
        if base == "max":
            mem_base = mem_max_max
            cpu_base = cpu_max_max
            base_label = "Max"
        elif base == "p95":
            mem_base = mem_p95_max if mem_p95_max > 0 else mem_max_max
            cpu_base = cpu_p95_max if cpu_p95_max > 0 else cpu_max_max
            base_label = "P95"
        elif base == "p90":
            mem_base = mem_p90_max if mem_p90_max > 0 else mem_max_max
            cpu_base = cpu_p90_max if cpu_p90_max > 0 else cpu_max_max
            base_label = "P90"
        elif base == "median":
            mem_base = mem_median_max if mem_median_max > 0 else mem_max_max
            cpu_base = cpu_median_max if cpu_median_max > 0 else cpu_max_max
            base_label = "Median"

        # Calculate recommendations: base + margin, but don't exceed max observed
        mem_recommended = (
            min(mem_max_max, int(mem_base * (1 + margin_pct / 100)))
            if mem_base > 0
            else mem_max_max
        )
        if cpu_base > 0:
            cpu_recommended = min(cpu_max_max * 1.1, cpu_base * (1 + margin_pct / 100))
        else:
            cpu_recommended = cpu_max_max if cpu_max_max > 0 else 0

        # Count coverage
        mem_coverage = len([x for x in mem_max_values if x <= mem_recommended])
        cpu_coverage = (
            len([x for x in cpu_max_values if x <= cpu_recommended]) if cpu_max_values else 0
        )

        all_recommendations[base] = {
            "step_name": step_name,
            "mem_recommended_mb": mem_recommended,
            "mem_recommended_k8s": mb_to_kubernetes(mem_recommended),
            "cpu_recommended_cores": cpu_recommended,
            "cpu_recommended_k8s": cores_to_kubernetes(cpu_recommended),
            "mem_max_max": mem_max_max,
            "mem_p95_max": mem_p95_max,
            "mem_p90_max": mem_p90_max,
            "mem_median_max": mem_median_max,
            "mem_base": mem_base,
            "cpu_max_max": cpu_max_max,
            "cpu_p95_max": cpu_p95_max,
            "cpu_p90_max": cpu_p90_max,
            "cpu_median_max": cpu_median_max,
            "cpu_base": cpu_base,
            "base_label": base_label,
            "mem_coverage": mem_coverage,
            "mem_total": len(mem_max_values),
            "cpu_coverage": cpu_coverage,
            "cpu_total": len(cpu_max_values) if cpu_max_values else 0,
        }

    return all_recommendations


def print_comparison_table(recommendations, current_resources=None, task_name=None, save_html=True):
    """Print comparison table of current vs proposed resource limits.

    Also saves the comparison table as HTML if task_name is provided and save_html is True.

    Args:
        recommendations: List of recommendation dictionaries
        current_resources: Dictionary of current resources by step name
        task_name: Optional task name for HTML file generation
        save_html: Whether to save HTML file (default: True). Set to False in Phase 1 to avoid
        duplicate files.

    Returns:
        Path to saved HTML file if task_name provided and save_html is True, None otherwise
    """
    if not recommendations:
        return None

    print("=" * 100)
    print("RESOURCE LIMITS COMPARISON: CURRENT vs PROPOSED")
    print("=" * 100)
    print()

    # Table header with reduced spacing
    print(
        f"{'Step':<15} {'Current Requests':<20} {'Proposed Requests':<20} "
        f"{'Current Limits':<20} {'Proposed Limits':<20}"
    )
    print("-" * 100)

    for rec in recommendations:
        if rec is None:
            continue

        step_name = rec["step_name"]
        # Convert step-build to build for matching
        step_name_yaml = normalize_step_name_for_compare(step_name)
        proposed_mem = rec["mem_recommended_k8s"]
        proposed_cpu = rec["cpu_recommended_k8s"]

        # Get current values
        if current_resources and step_name_yaml in current_resources:
            curr = current_resources[step_name_yaml]
            curr_mem_req = curr["requests"].get("memory") or "null"
            curr_cpu_req = curr["requests"].get("cpu") or "null"
            curr_mem_lim = curr["limits"].get("memory") or "null"
            curr_cpu_lim = curr["limits"].get("cpu") or "null"
        else:
            curr_mem_req = "N/A"
            curr_cpu_req = "N/A"
            curr_mem_lim = "N/A"
            curr_cpu_lim = "N/A"

        # Format values (memory / cpu)
        curr_req = f"{curr_mem_req} / {curr_cpu_req}"
        curr_lim = f"{curr_mem_lim} / {curr_cpu_lim}"
        prop_req = f"{proposed_mem} / {proposed_cpu}"
        prop_lim = f"{proposed_mem} / {proposed_cpu}"

        print(f"{step_name_yaml:<15} {curr_req:<20} {prop_req:<20} {curr_lim:<20} {prop_lim:<20}")

    print()

    # Save as HTML if task_name provided and save_html is True
    comparison_html_path = None
    if task_name and save_html:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        comparison_html_path = save_comparison_table_to_html(
            recommendations, current_resources, task_name, timestamp_str
        )
        if comparison_html_path:
            print(f"Saved comparison table as HTML: {comparison_html_path}", file=sys.stderr)

    return comparison_html_path


def print_analysis(
    recommendations,
    margin_pct,
    base="max",
    current_resources=None,
    task_name=None,
    save_comparison_html=True,
):
    """Print analysis results.

    Args:
        recommendations: List of recommendation dictionaries
        margin_pct: Margin percentage used
        base: Base metric used
        current_resources: Optional dictionary of current resources
        task_name: Optional task name for HTML file generation
        save_comparison_html: Whether to save comparison HTML file (default: True). Set to False in
        Phase 1 to avoid duplicate files.

    Returns:
        Path to comparison HTML file if saved, None otherwise
    """
    base_label = (
        recommendations[0]["base_label"] if recommendations and recommendations[0] else base.upper()
    )
    print("=" * 80)
    print(f"RESOURCE LIMIT RECOMMENDATIONS ({base_label} + {margin_pct}% Safety Margin)")
    print("=" * 80)
    print()

    for rec in recommendations:
        if rec is None:
            continue

        print(f"Step: {rec['step_name']}")
        print("-" * 80)
        print(f"  Memory: {rec['mem_recommended_k8s']}")
        if rec["base_label"] == "Max":
            print(f"    - Base ({rec['base_label']}): {mb_to_kubernetes(rec['mem_base'])}")
        else:
            print(f"    - Base ({rec['base_label']}): {mb_to_kubernetes(rec['mem_base'])}")
            print(f"    - Max observed: {mb_to_kubernetes(rec['mem_max_max'])}")
        print(f"    - Coverage: {rec['mem_coverage']}/{rec['mem_total']} clusters")
        print()

        if rec["cpu_total"] > 0:
            print(f"  CPU: {rec['cpu_recommended_k8s']}")
            if rec["base_label"] == "Max":
                print(f"    - Base ({rec['base_label']}): {cores_to_kubernetes(rec['cpu_base'])}")
            else:
                print(f"    - Base ({rec['base_label']}): {cores_to_kubernetes(rec['cpu_base'])}")
                print(f"    - Max observed: {cores_to_kubernetes(rec['cpu_max_max'])}")
            print(f"    - Coverage: {rec['cpu_coverage']}/{rec['cpu_total']} clusters")
        else:
            print("  CPU: No data available")
        print()

    # Print comparison table if current resources are available
    comparison_html_path = None
    if current_resources:
        print()
        comparison_html_path = print_comparison_table(
            recommendations, current_resources, task_name, save_html=save_comparison_html
        )

    return comparison_html_path


def get_cache_file_path(task_name):
    """Generate cache file path based on task name."""
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"
    cache_dir.mkdir(exist_ok=True)

    # Use task name as cache filename (sanitize for filesystem)
    # Replace any characters that might be problematic in filenames
    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    return cache_dir / f"{safe_task_name}.json"


def save_recommendations_cache(
    task_name, file_path_or_url, recommendations, margin_pct, base, days, csv_data=None
):
    """Save recommendations to cache file based on task name.

    Also saves CSV data and HTML files with timestamp for trend analysis.

    Args:
        task_name: Task name
        file_path_or_url: Original file path or URL
        recommendations: List of recommendation dictionaries
        margin_pct: Margin percentage used
        base: Base metric used
        days: Number of days analyzed
        csv_data: Optional CSV data string to save as HTML

    Returns:
        tuple: (cache_file_path, csv_html_path) - paths to saved files
    """
    cache_file = get_cache_file_path(task_name)
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    cache_data = {
        "task_name": task_name,
        "file_path_or_url": file_path_or_url,
        "timestamp": timestamp.isoformat(),
        "margin_pct": margin_pct,
        "base": base,
        "days": days,
        "recommendations": recommendations,
    }

    with open(cache_file, "w") as f:
        json.dump(cache_data, f, indent=2)

    print(f"Cached recommendations for task '{task_name}' to: {cache_file}", file=sys.stderr)

    # Save CSV as HTML if provided
    csv_html_path = None
    if csv_data:
        csv_html_path = save_csv_to_html(csv_data, task_name, timestamp_str)
        if csv_html_path:
            print(f"Saved CSV data as HTML: {csv_html_path}", file=sys.stderr)

    return cache_file, csv_html_path


def load_recommendations_cache(task_name):
    """Load recommendations from cache file based on task name."""
    cache_file = get_cache_file_path(task_name)

    if not cache_file.exists():
        return None

    try:
        with open(cache_file) as f:
            cache_data = json.load(f)

        # Verify it matches the requested task
        cached_task_name = cache_data.get("task_name")
        if cached_task_name == task_name:
            return cache_data
        else:
            print(
                f"Warning: Cache file exists but for different task '{cached_task_name}'."
                f" Ignoring cache.",
                file=sys.stderr,
            )
            return None
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Warning: Failed to load cache file: {e}", file=sys.stderr)
        return None


def save_csv_to_html(csv_data, task_name, timestamp_str):
    """Save CSV data as HTML table with sortable columns.

    Args:
        csv_data: CSV string with header and data rows
        task_name: Task name for filename
        timestamp_str: Timestamp string for filename (format: YYYYMMDD_HHMMSS)

    Returns:
        Path to saved HTML file
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"
    cache_dir.mkdir(exist_ok=True)

    # Sanitize task name for filename
    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    html_filename = f"{safe_task_name}_analyzed_data_{timestamp_str}.html"
    html_path = cache_dir / html_filename

    # Parse CSV using csv module for proper handling of quoted fields
    import io

    lines = [line.strip() for line in csv_data.strip().split("\n") if line.strip()]
    if not lines:
        return None

    # Parse CSV properly
    csv_reader = csv.reader(io.StringIO(csv_data))
    rows = list(csv_reader)

    if not rows:
        return None

    # First row is header
    headers = [h.strip().strip('"') for h in rows[0]]

    # Remaining rows are data
    data_rows = rows[1:] if len(rows) > 1 else []

    # Generate HTML
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resource Usage Data - {task_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            margin-bottom: 20px;
        }}
        .info {{
            margin-bottom: 20px;
            color: #666;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            cursor: pointer;
            user-select: none;
            position: relative;
        }}
        th:hover {{
            background-color: #45a049;
        }}
        th::after {{
            content: ' ↕';
            opacity: 0.5;
            margin-left: 5px;
        }}
        th.sorted-asc::after {{
            content: ' ↑';
            opacity: 1;
        }}
        th.sorted-desc::after {{
            content: ' ↓';
            opacity: 1;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
    </style>
    <script>
        function sortTable(columnIndex) {{
            const table = document.getElementById('dataTable');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const header = table.querySelectorAll('th')[columnIndex];
            const isAscending = header.classList.contains('sorted-asc');

            // Remove sort classes from all headers
            table.querySelectorAll('th').forEach(th => {{
                th.classList.remove('sorted-asc', 'sorted-desc');
            }});

            // Sort rows
            rows.sort((a, b) => {{
                const aText = a.cells[columnIndex].textContent.trim();
                const bText = b.cells[columnIndex].textContent.trim();

                // Try numeric comparison first
                const aNum = parseFloat(aText);
                const bNum = parseFloat(bText);
                if (!isNaN(aNum) && !isNaN(bNum)) {{
                    return isAscending ? bNum - aNum : aNum - bNum;
                }}

                // String comparison
                return isAscending ? bText.localeCompare(aText) : aText.localeCompare(bText);
            }});

            // Reorder rows
            rows.forEach(row => tbody.appendChild(row));

            // Add sort class to header
            header.classList.add(isAscending ? 'sorted-desc' : 'sorted-asc');
        }}
    </script>
</head>
<body>
    <h1>Resource Usage Data - {task_name}</h1>
    <div class="info">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
    <table id="dataTable">
        <thead>
            <tr>
"""

    # Add headers with click handlers
    for i, header in enumerate(headers):
        # Strip quotes from header names for cleaner display
        header_cleaned = header.strip().strip('"').strip("'")
        html_content += f'                <th onclick="sortTable({i})">{header_cleaned}</th>\n'

    html_content += """            </tr>
        </thead>
        <tbody>
"""

    # Add data rows
    for row in data_rows:
        html_content += "            <tr>\n"
        for cell in row:
            # Strip quotes from cell value for proper numeric sorting
            # CSV data has quotes around values, but HTML should display without quotes
            cell_cleaned = str(cell).strip().strip('"').strip("'")
            # Escape HTML special characters
            cell_escaped = (
                cell_cleaned.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )
            html_content += f"                <td>{cell_escaped}</td>\n"
        html_content += "            </tr>\n"

    html_content += """        </tbody>
    </table>
</body>
</html>
"""

    # Save file
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    return html_path


def save_comparison_table_to_html(recommendations, current_resources, task_name, timestamp_str):
    """Save comparison table as HTML (non-sortable).

    Args:
        recommendations: List of recommendation dictionaries
        current_resources: Dictionary of current resources by step name
        task_name: Task name for filename
        timestamp_str: Timestamp string for filename (format: YYYYMMDD_HHMMSS)

    Returns:
        Path to saved HTML file
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"
    cache_dir.mkdir(exist_ok=True)

    # Sanitize task name for filename
    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    html_filename = f"{safe_task_name}_comparison_data_{timestamp_str}.html"
    html_path = cache_dir / html_filename

    # Generate HTML
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resource Limits Comparison - {task_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            margin-bottom: 20px;
        }}
        .info {{
            margin-bottom: 20px;
            color: #666;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #2196F3;
            color: white;
            padding: 12px;
            text-align: left;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
    </style>
</head>
<body>
    <h1>Resource Limits Comparison: Current vs Proposed</h1>
    <div class="info">Task: {task_name}</div>
    <div class="info">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
    <table>
        <thead>
            <tr>
                <th>Step</th>
                <th>Current Requests</th>
                <th>Proposed Requests</th>
                <th>Current Limits</th>
                <th>Proposed Limits</th>
            </tr>
        </thead>
        <tbody>
"""

    # Add data rows
    for rec in recommendations:
        if rec is None:
            continue

        step_name = rec["step_name"]
        step_name_yaml = normalize_step_name_for_compare(step_name)
        proposed_mem = rec["mem_recommended_k8s"]
        proposed_cpu = rec["cpu_recommended_k8s"]

        # Get current values
        if current_resources and step_name_yaml in current_resources:
            curr = current_resources[step_name_yaml]
            curr_mem_req = curr["requests"].get("memory") or "null"
            curr_cpu_req = curr["requests"].get("cpu") or "null"
            curr_mem_lim = curr["limits"].get("memory") or "null"
            curr_cpu_lim = curr["limits"].get("cpu") or "null"
        else:
            curr_mem_req = "N/A"
            curr_cpu_req = "N/A"
            curr_mem_lim = "N/A"
            curr_cpu_lim = "N/A"

        # Format values
        curr_req = f"{curr_mem_req} / {curr_cpu_req}"
        curr_lim = f"{curr_mem_lim} / {curr_cpu_lim}"
        prop_req = f"{proposed_mem} / {proposed_cpu}"
        prop_lim = f"{proposed_mem} / {proposed_cpu}"

        html_content += f"""            <tr>
                <td>{step_name_yaml}</td>
                <td>{curr_req}</td>
                <td>{prop_req}</td>
                <td>{curr_lim}</td>
                <td>{prop_lim}</td>
            </tr>
"""

    html_content += """        </tbody>
    </table>
</body>
</html>
"""

    # Save file
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    return html_path


def get_date_based_file_path(task_name, file_type, date_str, timestamp_str=None, margin_pct=None):
    """Get file path for date-based file naming.

    Args:
        task_name: Task name
        file_type: 'analyzed_data' or 'comparison_data'
        date_str: Date string in YYYYMMDD format
        timestamp_str: Optional timestamp string in YYYYMMDD_HHMMSS format (only used for re-
        analysis)
        margin_pct: Optional margin percentage (only used for comparison_data file type)

    Returns:
        Path object for the file
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"
    cache_dir.mkdir(exist_ok=True)

    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)

    if file_type == "comparison_data" and margin_pct is not None:
        # Comparison files include margin in filename
        if timestamp_str:
            filename = f"{safe_task_name}_{file_type}_margin-{margin_pct}_{timestamp_str}"
        else:
            filename = f"{safe_task_name}_{file_type}_margin-{margin_pct}_{date_str}"
    else:
        # Analyzed data files don't include margin
        if timestamp_str:
            filename = f"{safe_task_name}_{file_type}_{timestamp_str}"
        else:
            filename = f"{safe_task_name}_{file_type}_{date_str}"

    return cache_dir / filename


def check_files_exist_for_date(task_name, file_type, date_str, margin_pct=None):
    """Check if files already exist for a given date.

    Args:
        task_name: Task name
        file_type: 'analyzed_data' or 'comparison_data'
        date_str: Date string in YYYYMMDD format
        margin_pct: Optional margin percentage (only used for comparison_data)

    Returns:
        True if files exist for this date, False otherwise
    """
    html_path = get_date_based_file_path(
        task_name, file_type, date_str, margin_pct=margin_pct
    ).with_suffix(".html")
    json_path = get_date_based_file_path(
        task_name, file_type, date_str, margin_pct=margin_pct
    ).with_suffix(".json")
    return html_path.exists() or json_path.exists()


def check_comparison_file_exists_for_margin(task_name, date_str, margin_pct):
    """Check if comparison file exists for a specific margin.

    Args:
        task_name: Task name
        date_str: Date string in YYYYMMDD format
        margin_pct: Margin percentage

    Returns:
        True if comparison file exists for this margin, False otherwise
    """
    # Check date-only format first
    html_path = get_date_based_file_path(
        task_name, "comparison_data", date_str, margin_pct=margin_pct
    ).with_suffix(".html")
    json_path = get_date_based_file_path(
        task_name, "comparison_data", date_str, margin_pct=margin_pct
    ).with_suffix(".json")

    if html_path.exists() or json_path.exists():
        return True

    # Also check if any timestamped version exists for this margin
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"

    if not cache_dir.exists():
        return False

    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    pattern = f"{safe_task_name}_comparison_data_margin-{margin_pct}_{date_str}_*.json"

    matching_files = list(cache_dir.glob(pattern))
    return len(matching_files) > 0


def save_analyzed_data(
    task_name,
    csv_data,
    date_str,
    steps_without_observability_data=None,
    cluster_coverage_report=None,
    days_requested=None,
):
    """Save analyzed data (CSV) as HTML and JSON files.

    Args:
        task_name: Task name
        csv_data: CSV data string
        date_str: Date string in YYYYMMDD format
        steps_without_observability_data: Optional list of YAML step names with no Prometheus rows
        cluster_coverage_report: Optional dict from compute_cluster_coverage_report()
        days_requested: Number of days requested (for coverage banner)

    Returns:
        tuple: (html_path, json_path) - paths to saved files
    """
    # If files exist for this date, use timestamp to preserve old files
    if check_files_exist_for_date(task_name, "analyzed_data", date_str):
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = get_date_based_file_path(
            task_name, "analyzed_data", date_str, timestamp_str
        ).with_suffix(".html")
        json_path = get_date_based_file_path(
            task_name, "analyzed_data", date_str, timestamp_str
        ).with_suffix(".json")
    else:
        html_path = get_date_based_file_path(task_name, "analyzed_data", date_str).with_suffix(
            ".html"
        )
        json_path = get_date_based_file_path(task_name, "analyzed_data", date_str).with_suffix(
            ".json"
        )

    # Save HTML (reuse existing function but with date-based naming)
    if csv_data:
        import io

        csv_reader = csv.reader(io.StringIO(csv_data))
        rows = list(csv_reader)

        if rows:
            headers = [h.strip().strip('"') for h in rows[0]]
            data_rows = rows[1:] if len(rows) > 1 else []
            banner_html = _html_steps_missing_observability_banner(steps_without_observability_data)
            coverage_banner = _html_cluster_coverage_banner(
                cluster_coverage_report or {}, days_requested or 0
            )
            scrape_note = _html_scrape_interval_note()

            html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resource Usage Data - {task_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            margin-bottom: 20px;
        }}
        .info {{
            margin-bottom: 20px;
            color: #666;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            cursor: pointer;
            user-select: none;
            position: relative;
        }}
        th:hover {{
            background-color: #45a049;
        }}
        th::after {{
            content: ' ↕';
            opacity: 0.5;
            margin-left: 5px;
        }}
        th.sorted-asc::after {{
            content: ' ↑';
            opacity: 1;
        }}
        th.sorted-desc::after {{
            content: ' ↓';
            opacity: 1;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
    </style>
    <script>
        function sortTable(columnIndex) {{
            const table = document.getElementById('dataTable');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const header = table.querySelectorAll('th')[columnIndex];
            const isAscending = header.classList.contains('sorted-asc');

            table.querySelectorAll('th').forEach(th => {{
                th.classList.remove('sorted-asc', 'sorted-desc');
            }});

            rows.sort((a, b) => {{
                const aText = a.cells[columnIndex].textContent.trim();
                const bText = b.cells[columnIndex].textContent.trim();

                const aNum = parseFloat(aText);
                const bNum = parseFloat(bText);
                if (!isNaN(aNum) && !isNaN(bNum)) {{
                    return isAscending ? bNum - aNum : aNum - bNum;
                }}

                return isAscending ? bText.localeCompare(aText) : aText.localeCompare(bText);
            }});

            rows.forEach(row => tbody.appendChild(row));
            header.classList.add(isAscending ? 'sorted-desc' : 'sorted-asc');
        }}
    </script>
</head>
<body>
    <h1>Resource Usage Data - {task_name}</h1>
    <div class="info">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
{coverage_banner}{scrape_note}{banner_html}    <table id="dataTable">
        <thead>
            <tr>
"""

            for i, header in enumerate(headers):
                header_cleaned = header.strip().strip('"').strip("'")
                html_content += (
                    f'                <th onclick="sortTable({i})">{header_cleaned}</th>\n'
                )

            html_content += """            </tr>
        </thead>
        <tbody>
"""

            for row in data_rows:
                html_content += "            <tr>\n"
                for cell in row:
                    cell_cleaned = str(cell).strip().strip('"').strip("'")
                    cell_escaped = (
                        cell_cleaned.replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                        .replace('"', "&quot;")
                    )
                    html_content += f"                <td>{cell_escaped}</td>\n"
                html_content += "            </tr>\n"

            html_content += """        </tbody>
    </table>
</body>
</html>
"""

            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)

    # Save JSON (convert CSV to JSON structure)
    json_data = {
        "task_name": task_name,
        "date": date_str,
        "timestamp": datetime.now().isoformat(),
        "csv_data": csv_data,
        "steps_without_observability_data": list(steps_without_observability_data or []),
        "cluster_coverage_report": cluster_coverage_report or {},
        "days_requested": days_requested or 0,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)

    return html_path, json_path


def _write_one_step_detailed_files(
    cache_dir, task_name, step_name, step_executions, date_str, timestamp_suffix=None
):
    """Write HTML, JSON, and CSV for a single step.

    Used by save_detailed_per_step_data.
    Column order: Cluster, Namespace, Task, Step, Component, Application, Pod,
    Final Memory Usage (MB), Current Mem requests, Current Mem Limits,
    Final CPU Usage (Cores), Current CPU requests, Current CPU Limits, Timestamp.
    Step name is displayed without 'step-' prefix. Current * values are Kubernetes format (e.g.
    512Mi, 1000m).

    Returns:
        tuple: (html_path, json_path, csv_path)
    """
    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    safe_step_name = re.sub(r"[^a-zA-Z0-9_-]", "_", step_name)
    if timestamp_suffix:
        base = (
            f"{safe_task_name}_analyzed_data_detailed_step"
            f"_{safe_step_name}_{date_str}"
            f"_{timestamp_suffix}"
        )
    else:
        base = f"{safe_task_name}_analyzed_data_detailed_step_{safe_step_name}_{date_str}"
    html_path = cache_dir / f"{base}.html"
    json_path = cache_dir / f"{base}.json"
    csv_path = cache_dir / f"{base}.csv"

    table_id = f"table_{safe_step_name}"
    # Numeric columns for sort: 7 = Final Memory, 10 = Final CPU, 11 = I/O Read, 12 = I/O Write
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Detailed Resource Usage - {task_name} / {step_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        h1 {{ color: #333; margin-bottom: 10px; }}
        .info {{ margin-bottom: 20px; color: #666; }}
        .table-wrapper {{ overflow-x: auto; width: 100%; }}
        table {{ border-collapse: collapse; min-width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        th {{ background-color: #4CAF50; color: white;
            padding: 12px; text-align: left;
            cursor: pointer; user-select: none;
            position: relative; white-space: normal;
            word-wrap: break-word; max-width: 120px; }}
        th:hover {{ background-color: #45a049; }}
        th::after {{ content: ' ↕'; opacity: 0.5; margin-left: 5px; }}
        th.sorted-asc::after {{ content: ' ↑'; opacity: 1; }}
        th.sorted-desc::after {{ content: ' ↓'; opacity: 1; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background-color: #f5f5f5; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .io-high {{ background-color: #fff3cd !important; font-weight: bold; }}
    </style>
    <script>
        function sortTable(tableId, columnIndex) {{
            const table = document.getElementById(tableId);
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const header = table.querySelectorAll('th')[columnIndex];
            const isAscending = header.classList.contains('sorted-asc');
            const isNumeric = (columnIndex === 7
                || columnIndex === 10
                || columnIndex === 11
                || columnIndex === 12);
            table.querySelectorAll('th').forEach(
                th => th.classList.remove(
                    'sorted-asc', 'sorted-desc'));
            rows.sort((a, b) => {{
                const aText = a.cells[columnIndex].textContent.trim();
                const bText = b.cells[columnIndex].textContent.trim();
                if (isNumeric) {{
                    const aNum = parseFloat(aText);
                    const bNum = parseFloat(bText);
                    if (!isNaN(aNum) && !isNaN(bNum))
                        return isAscending
                            ? bNum - aNum : aNum - bNum;
                }}
                return isAscending ? bText.localeCompare(aText) : aText.localeCompare(bText);
            }});
            rows.forEach(row => tbody.appendChild(row));
            header.classList.add(isAscending ? 'sorted-desc' : 'sorted-asc');
        }}
    </script>
</head>
<body>
    <h1>Historical Resource Utilization: {task_name} &ndash; {step_name}</h1>
    <div class="info">Task: {task_name}</div>
    <div class="info">Step: {step_name}</div>
    <div class="info">Pod Executions: {len(step_executions)}</div>
    <div class="info">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
    <div class="table-wrapper">
    <table id="{table_id}">
        <thead>
            <tr>
                <th onclick="sortTable('{table_id}', 0)">Cluster</th>
                <th onclick="sortTable('{table_id}', 1)">Namespace</th>
                <th onclick="sortTable('{table_id}', 2)">Task</th>
                <th onclick="sortTable('{table_id}', 3)">Step</th>
                <th onclick="sortTable('{table_id}', 4)">Component</th>
                <th onclick="sortTable('{table_id}', 5)">Application</th>
                <th onclick="sortTable('{table_id}', 6)">Pod</th>
                <th onclick="sortTable('{table_id}', 7)">Final Memory Usage (MB)</th>
                <th onclick="sortTable('{table_id}', 8)">Current Mem requests</th>
                <th onclick="sortTable('{table_id}', 9)">Current Mem Limits</th>
                <th onclick="sortTable('{table_id}', 10)">Final CPU Usage (Cores)</th>
                <th onclick="sortTable('{table_id}', 11)">Peak Disk Read (MB/s)</th>
                <th onclick="sortTable('{table_id}', 12)">Peak Disk Write (MB/s)</th>
                <th onclick="sortTable('{table_id}', 13)">Current CPU requests</th>
                <th onclick="sortTable('{table_id}', 14)">Current CPU Limits</th>
                <th onclick="sortTable('{table_id}', 15)">Timestamp</th>
            </tr>
        </thead>
        <tbody>
"""
    IO_HIGH_THRESHOLD_MBPS = 50.0  # highlight rows where I/O exceeds this value
    for exec_data in sorted(
        step_executions,
        key=lambda x: (x.get("cluster", ""), x.get("namespace", ""), x.get("timestamp", "")),
    ):
        app = exec_data.get("application", "N/A")
        mem_req = exec_data.get("mem_requests_k8s", "N/A")
        mem_lim = exec_data.get("mem_limits_k8s", "N/A")
        cpu_req = exec_data.get("cpu_requests_k8s", "N/A")
        cpu_lim = exec_data.get("cpu_limits_k8s", "N/A")
        io_read = exec_data.get("io_read_mbps", 0)
        io_write = exec_data.get("io_write_mbps", 0)
        io_class = (
            ' class="io-high"'
            if (io_read >= IO_HIGH_THRESHOLD_MBPS or io_write >= IO_HIGH_THRESHOLD_MBPS)
            else ""
        )
        html_content += f"""        <tr{io_class}>
            <td>{exec_data.get("cluster", "N/A")}</td>
            <td>{exec_data.get("namespace", "N/A")}</td>
            <td>{exec_data.get("task", "N/A")}</td>
            <td>{exec_data.get("step", "N/A")}</td>
            <td>{exec_data.get("component", "N/A")}</td>
            <td>{app}</td>
            <td>{exec_data.get("pod", "N/A")}</td>
            <td>{exec_data.get("memory_mb", 0)}</td>
            <td>{mem_req}</td>
            <td>{mem_lim}</td>
            <td>{exec_data.get("cpu_cores", 0)}</td>
            <td>{io_read}</td>
            <td>{io_write}</td>
            <td>{cpu_req}</td>
            <td>{cpu_lim}</td>
            <td>{exec_data.get("timestamp", "N/A")}</td>
        </tr>
"""
    html_content += """        </tbody>
    </table>
    </div>
</body>
</html>
"""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    json_data = {
        "task_name": task_name,
        "step_name": step_name,
        "date": date_str,
        "timestamp": datetime.now().isoformat(),
        "executions_count": len(step_executions),
        "executions": step_executions,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)

    csv_header = (
        '"cluster","namespace","task","step","component","application","pod",'
        '"final_memory_mb","current_mem_requests","current_mem_limits",'
        '"final_cpu_cores","io_read_mbps","io_write_mbps",'
        '"current_cpu_requests","current_cpu_limits","timestamp"'
    )
    csv_lines = [csv_header]
    for exec_data in step_executions:
        csv_lines.append(
            f'"{exec_data.get("cluster", "")}",'
            f'"{exec_data.get("namespace", "")}",'
            f'"{exec_data.get("task", "")}",'
            f'"{exec_data.get("step", "")}",'
            f'"{exec_data.get("component", "N/A")}",'
            f'"{exec_data.get("application", "N/A")}",'
            f'"{exec_data.get("pod", "")}",'
            f"{exec_data.get('memory_mb', 0)},"
            f'"{exec_data.get("mem_requests_k8s", "N/A")}",'
            f'"{exec_data.get("mem_limits_k8s", "N/A")}",'
            f"{exec_data.get('cpu_cores', 0)},"
            f"{exec_data.get('io_read_mbps', 0)},"
            f"{exec_data.get('io_write_mbps', 0)},"
            f'"{exec_data.get("cpu_requests_k8s", "N/A")}",'
            f'"{exec_data.get("cpu_limits_k8s", "N/A")}",'
            f'"{exec_data.get("timestamp", "")}"'
        )
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("\n".join(csv_lines))

    return html_path, json_path, csv_path


def save_detailed_per_step_data(task_name, executions_data, date_str):
    """Save detailed per-step pod execution data as one HTML, JSON, and CSV per step.

    Filenames: {task}_analyzed_data_detailed_step_{step_name}_{date}[_{time}].html/json/csv

    Args:
        task_name: Task name
        executions_data: List of pod execution dictionaries
        date_str: Date string in YYYYMMDD format

    Returns:
        list of tuples: [(html_path, json_path, csv_path), ...] one per step
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"
    cache_dir.mkdir(exist_ok=True)
    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)

    by_step = defaultdict(list)
    for exec_data in executions_data:
        step = exec_data.get("step", "")
        if step:
            by_step[step].append(exec_data)

    # Decide timestamp suffix: if any step's files already exist for this date, add timestamp
    timestamp_suffix = None
    for step_name in by_step:
        safe_step = re.sub(r"[^a-zA-Z0-9_-]", "_", step_name)
        base = f"{safe_task_name}_analyzed_data_detailed_step_{safe_step}_{date_str}"
        if (cache_dir / f"{base}.html").exists() or (cache_dir / f"{base}.json").exists():
            # Time only (date already in base) to avoid ..._20260217_20260217_180906
            timestamp_suffix = datetime.now().strftime("%H%M%S")
            break

    result_paths = []
    for step_name in sorted(by_step.keys()):
        step_executions = by_step[step_name]
        html_path, json_path, csv_path = _write_one_step_detailed_files(
            cache_dir, task_name, step_name, step_executions, date_str, timestamp_suffix
        )
        result_paths.append((html_path, json_path, csv_path))
    return result_paths


def split_existing_detailed_per_step_json_to_per_step_files(json_path):
    """One-time helper: read a combined detailed_per_step JSON and write one HTML/JSON/CSV per step.

    Args:
        json_path: Path to existing {task}_analyzed_data_detailed_per_step_{date}.json

    Returns:
        list of (html_path, json_path, csv_path) per step
    """
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(json_path)
    cache_dir = path.parent
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    task_name = data.get("task_name", "")
    date_str = data.get("date", "")
    executions_by_step = data.get("executions_by_step", {})
    if not task_name or not date_str or not executions_by_step:
        raise ValueError("JSON missing task_name, date, or executions_by_step")
    result_paths = []
    for step_name in sorted(executions_by_step.keys()):
        step_executions = executions_by_step[step_name]
        html_path, json_path, csv_path = _write_one_step_detailed_files(
            cache_dir, task_name, step_name, step_executions, date_str, timestamp_suffix=None
        )
        result_paths.append((html_path, json_path, csv_path))
    return result_paths


def save_comparison_data_all_bases(
    task_name,
    all_recommendations_by_base,
    current_resources,
    margin_pct,
    date_str,
    use_timestamp=False,
    steps_without_observability_data=None,
    cluster_coverage_report=None,
    days_requested=None,
    detailed_executions=None,
):
    """Save comparison data for all base metrics as HTML and JSON.

    Args:
        task_name: Task name
        all_recommendations_by_base: Dict with keys 'max', 'p95', 'p90', 'median', each containing
        list of recommendations
        current_resources: Dictionary of current resources by step name
        margin_pct: Margin percentage used
        date_str: Date string in YYYYMMDD format
        use_timestamp: If True, add timestamp to filename (used when re-analysis happens in Phase 1)
        steps_without_observability_data: Optional list of YAML steps with no Prometheus rows (Phase
        1 only)
        cluster_coverage_report: Optional dict from compute_cluster_coverage_report()
        days_requested: Number of days requested (for coverage banner)
        detailed_executions: Optional list of per-pod execution dicts (enables violators sub-tables)

    Returns:
        tuple: (html_path, json_path) - paths to saved files
    """
    # If re-analysis happened (use_timestamp=True), use timestamp
    # Otherwise, just use date (for Phase 1 first run or Phase 2 with different margin)
    if use_timestamp:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = get_date_based_file_path(
            task_name, "comparison_data", date_str, timestamp_str, margin_pct
        ).with_suffix(".html")
        json_path = get_date_based_file_path(
            task_name, "comparison_data", date_str, timestamp_str, margin_pct
        ).with_suffix(".json")
    else:
        html_path = get_date_based_file_path(
            task_name, "comparison_data", date_str, margin_pct=margin_pct
        ).with_suffix(".html")
        json_path = get_date_based_file_path(
            task_name, "comparison_data", date_str, margin_pct=margin_pct
        ).with_suffix(".json")

    banner_cmp = _html_steps_missing_observability_banner(steps_without_observability_data or [])
    coverage_banner = _html_cluster_coverage_banner(
        cluster_coverage_report or {}, days_requested or 0
    )
    tail_warnings = compute_heavy_tail_warnings(all_recommendations_by_base)
    tail_banner = _html_heavy_tail_warnings_banner(tail_warnings)
    scrape_note = _html_scrape_interval_note()
    # Generate HTML with separate tables for each base metric
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resource Limits Comparison - {task_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            margin-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
            margin-bottom: 15px;
            border-bottom: 2px solid #2196F3;
            padding-bottom: 5px;
        }}
        .info {{
            margin-bottom: 20px;
            color: #666;
        }}
        /* main comparison table */
        table.main-cmp {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 4px;
        }}
        table.main-cmp th {{
            background-color: #2196F3;
            color: white;
            padding: 11px 12px;
            text-align: left;
        }}
        table.main-cmp td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ddd;
        }}
        table.main-cmp tr:hover {{ background-color: #f0f7ff; }}
        table.main-cmp tr:nth-child(even) {{ background-color: #f9f9f9; }}

        /* violators collapsible block */
        details.violators-block {{
            background: #fffbf0;
            border: 1px solid #ffe082;
            border-radius: 4px;
            margin-bottom: 20px;
        }}
        summary.violators-summary {{
            cursor: pointer;
            padding: 8px 14px;
            font-size: 0.92em;
            color: #7b4f00;
            list-style: none;
            user-select: none;
        }}
        summary.violators-summary::-webkit-details-marker {{ display: none; }}
        summary.violators-summary:hover {{ background: #fff3cd; border-radius: 4px; }}
        .violators-body {{ padding: 0 14px 14px; }}
        .no-violators {{
            color: #388e3c;
            font-size: 0.88em;
            padding: 4px 0 16px 2px;
            font-style: italic;
            margin: 0 0 16px 0;
        }}

        /* violators inner table */
        table.violators-table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 0.88em;
            background: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        }}
        table.violators-table th {{
            background: #fff3cd;
            color: #5d4037;
            padding: 8px 10px;
            text-align: left;
            border-bottom: 2px solid #ffe082;
        }}
        table.violators-table td {{
            padding: 7px 10px;
            border-bottom: 1px solid #f0f0f0;
            vertical-align: middle;
        }}
        td.viol-ns-cell {{
            font-weight: bold;
            color: #1565c0;
            border-right: 2px solid #bbdefb;
            background: #e3f2fd;
        }}
        td.viol-app-cell {{
            color: #2e7d32;
            border-right: 1px solid #c8e6c9;
            background: #f1f8f1;
        }}
        td.viol-cell {{ color: #c62828; font-weight: bold; }}
        .cluster-badge {{
            background: #e8eaf6;
            color: #303f9f;
            font-size: 0.82em;
            padding: 2px 6px;
            border-radius: 3px;
            white-space: nowrap;
        }}
        /* sortable violators column headers */
        table.violators-table th.sortable {{
            cursor: pointer;
            user-select: none;
            white-space: nowrap;
            background: #ffe082;
        }}
        table.violators-table th.sortable:hover {{ background: #ffd54f; }}
        table.violators-table th.sort-asc::after  {{ content: " ↑"; font-weight: bold; }}
        table.violators-table th.sort-desc::after {{ content: " ↓"; font-weight: bold; }}
        .sort-hint {{ font-size: 0.78em; color: #888; margin: 4px 0 6px 0; }}
    </style>
    <script>
    function sortVT(tableId, colIdx) {{
        var tbl   = document.getElementById(tableId);
        var ths   = tbl.querySelectorAll('thead th');
        var tbody = tbl.querySelector('tbody');
        var rows  = Array.from(tbody.querySelectorAll('tr'));
        var th    = ths[colIdx];
        var asc   = th.classList.contains('sort-desc');
        ths.forEach(function(h) {{ h.classList.remove('sort-asc', 'sort-desc'); }});
        rows.sort(function(a, b) {{
            var av = parseFloat(a.cells[colIdx].getAttribute('data-val'));
            var bv = parseFloat(b.cells[colIdx].getAttribute('data-val'));
            if (!isNaN(av) && !isNaN(bv)) return asc ? av - bv : bv - av;
            var at = a.cells[colIdx].textContent.trim();
            var bt = b.cells[colIdx].textContent.trim();
            return asc ? at.localeCompare(bt) : bt.localeCompare(at);
        }});
        rows.forEach(function(r) {{ tbody.appendChild(r); }});
        th.classList.add(asc ? 'sort-asc' : 'sort-desc');
    }}
    </script>
</head>
<body>
    <h1>Resource Limits Comparison: Current vs Proposed</h1>
    <div class="info">Task: {task_name}</div>
    <div class="info">Margin: {margin_pct}%</div>
    <div class="info">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
{coverage_banner}{tail_banner}{scrape_note}{banner_cmp}
"""

    # Generate table + violators for each base metric
    for base in ["max", "p95", "p90", "median"]:
        recommendations = all_recommendations_by_base.get(base, [])
        if not recommendations:
            continue

        base_label = base.upper() if base != "max" else "MAX"
        html_content += f"""
    <h2>Base Metric: {base_label} (Margin: {margin_pct}%)</h2>
    <table class="main-cmp">
        <thead>
            <tr>
                <th>Step</th>
                <th>Current Requests<br><small>(mem / cpu)</small></th>
                <th>Proposed Requests<br><small>(mem / cpu)</small></th>
                <th>Current Limits<br><small>(mem / cpu)</small></th>
                <th>Proposed Limits<br><small>(mem / cpu)</small></th>
            </tr>
        </thead>
        <tbody>
"""

        for rec in recommendations:
            if rec is None:
                continue

            step_name = rec["step_name"]
            step_name_yaml = normalize_step_name_for_compare(step_name)
            proposed_mem = rec["mem_recommended_k8s"]
            proposed_cpu = rec["cpu_recommended_k8s"]

            if current_resources and step_name_yaml in current_resources:
                curr = current_resources[step_name_yaml]
                curr_mem_req = curr["requests"].get("memory") or "null"
                curr_cpu_req = curr["requests"].get("cpu") or "null"
                curr_mem_lim = curr["limits"].get("memory") or "null"
                curr_cpu_lim = curr["limits"].get("cpu") or "null"
            else:
                curr_mem_req = "N/A"
                curr_cpu_req = "N/A"
                curr_mem_lim = "N/A"
                curr_cpu_lim = "N/A"

            curr_req = f"{curr_mem_req} / {curr_cpu_req}"
            curr_lim = f"{curr_mem_lim} / {curr_cpu_lim}"
            prop_req = f"{proposed_mem} / {proposed_cpu}"
            prop_lim = f"{proposed_mem} / {proposed_cpu}"

            html_content += f"""            <tr>
                <td><strong>{step_name_yaml}</strong></td>
                <td>{curr_req}</td>
                <td>{prop_req}</td>
                <td>{curr_lim}</td>
                <td>{prop_lim}</td>
            </tr>
"""

        html_content += "        </tbody>\n    </table>\n"

        # Violators block (one per step, only when detailed_executions are available)
        if detailed_executions:
            for rec in recommendations:
                if rec is None:
                    continue
                step_name = rec["step_name"]
                step_disp = normalize_step_name_for_compare(step_name)
                mem_base = rec.get("mem_base", 0)
                cpu_base = rec.get("cpu_base", 0)
                viol = _compute_violators_for_step(
                    detailed_executions, step_name, mem_base, cpu_base
                )
                html_content += _html_violators_block(
                    viol, mem_base, cpu_base, base_label, step_disp
                )

    html_content += """</body>
</html>
"""

    # Save HTML
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Save JSON
    json_data = {
        "task_name": task_name,
        "date": date_str,
        "timestamp": datetime.now().isoformat(),
        "margin_pct": margin_pct,
        "recommendations_by_base": all_recommendations_by_base,
        "steps_without_observability_data": list(steps_without_observability_data or []),
        "cluster_coverage_report": cluster_coverage_report or {},
        "days_requested": days_requested or 0,
        "heavy_tail_warnings": tail_warnings,
        "current_resources": current_resources or {},
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)

    return html_path, json_path


def load_analyzed_data(task_name, date_str):
    """Load analyzed data from JSON file.

    Tries date-only format first, then looks for latest date+timestamp format.

    Args:
        task_name: Task name
        date_str: Date string in YYYYMMDD format

    Returns:
        Dictionary with analyzed data or None if not found
    """
    # Try date-only format first
    json_path = get_date_based_file_path(task_name, "analyzed_data", date_str).with_suffix(".json")

    if json_path.exists():
        try:
            with open(json_path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load analyzed data: {e}", file=sys.stderr)
            return None

    # If not found, look for latest date+timestamp format
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"

    if not cache_dir.exists():
        return None

    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    pattern = f"{safe_task_name}_analyzed_data_{date_str}_*.json"

    matching_files = list(cache_dir.glob(pattern))
    if not matching_files:
        return None

    # Sort by modification time (newest first) and load the latest
    matching_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    latest_file = matching_files[0]

    try:
        with open(latest_file, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Warning: Failed to load analyzed data: {e}", file=sys.stderr)
        return None


def load_comparison_data(task_name, date_str, margin_pct):
    """Load comparison data from JSON file for a specific margin.

    Tries date-only format first, then looks for latest date+timestamp format.

    Args:
        task_name: Task name
        date_str: Date string in YYYYMMDD format
        margin_pct: Margin percentage

    Returns:
        Dictionary with comparison data or None if not found
    """
    # Try date-only format first
    json_path = get_date_based_file_path(
        task_name, "comparison_data", date_str, margin_pct=margin_pct
    ).with_suffix(".json")

    if json_path.exists():
        try:
            with open(json_path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load comparison data: {e}", file=sys.stderr)
            return None

    # If not found, look for latest date+timestamp format with this margin
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"

    if not cache_dir.exists():
        return None

    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    pattern = f"{safe_task_name}_comparison_data_margin-{margin_pct}_{date_str}_*.json"

    matching_files = list(cache_dir.glob(pattern))
    if not matching_files:
        return None

    # Sort by modification time (newest first) and load the latest
    matching_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    latest_file = matching_files[0]

    try:
        with open(latest_file, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Warning: Failed to load comparison data: {e}", file=sys.stderr)
        return None


def find_latest_analysis_date(task_name):
    """Find the latest analysis date for a task.

    Handles both date-only (YYYYMMDD) and date+timestamp (YYYYMMDD_HHMMSS) formats.

    Args:
        task_name: Task name

    Returns:
        Date string in YYYYMMDD format or None if not found
    """
    script_dir = Path(__file__).parent
    cache_dir = script_dir / ".analyze_cache"

    if not cache_dir.exists():
        return None

    safe_task_name = re.sub(r"[^a-zA-Z0-9_-]", "_", task_name)
    pattern = f"{safe_task_name}_analyzed_data_*.json"

    matching_files = list(cache_dir.glob(pattern))
    if not matching_files:
        return None

    # Extract dates and find the latest
    dates = set()
    for file_path in matching_files:
        # Extract date from filename: task_analyzed_data_YYYYMMDD.json or
        # task_analyzed_data_YYYYMMDD_HHMMSS.json
        match = re.search(r"_analyzed_data_(\d{8})(?:_\d{6})?\.json$", file_path.name)
        if match:
            dates.add(match.group(1))  # Extract just the date part (YYYYMMDD)

    if not dates:
        return None

    # Return the latest date (already in YYYYMMDD format, so lexicographic sort works)
    return max(dates)


def extract_component_from_pod(pod_name, namespace, token, prom_host, end_time, days):
    """Extract component and application from pod labels or namespace/pod name.

    Args:
        pod_name: Pod name
        namespace: Namespace
        token: Prometheus token
        prom_host: Prometheus host
        end_time: End time for query
        days: Number of days

    Returns:
        Tuple (component, application); each is a string or "N/A"
    """
    try:
        # Try using get_component_for_pod.py script first
        script_dir = Path(__file__).parent
        component_script = script_dir / "get_component_for_pod.py"

        if component_script.exists():
            result = subprocess.run(
                [
                    sys.executable,
                    str(component_script),
                    token,
                    prom_host,
                    pod_name,
                    namespace or "N/A",
                    str(end_time),
                    str(days),
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                try:
                    component_data = json.loads(result.stdout)
                    component = component_data.get("component", "N/A")
                    application = component_data.get("application", "N/A")
                    if component and component != "N/A":
                        return (component, application if application else "N/A")
                except (json.JSONDecodeError, KeyError):
                    pass

        # Fallback: Try to extract from namespace/pod name
        component_fallback = "N/A"
        if namespace and namespace != "N/A" and namespace.endswith("-tenant"):
            potential_component = namespace[:-7]  # Remove "-tenant"
            if potential_component:
                component_fallback = potential_component
        if component_fallback == "N/A" and pod_name:
            parts = pod_name.split("-")
            if len(parts) >= 2:
                potential_component = parts[0]
                if potential_component and len(potential_component) > 1:
                    component_fallback = potential_component

        return (component_fallback, "N/A")
    except Exception as e:
        if globals().get("args") and globals()["args"].debug:
            print(f"DEBUG: Error extracting component for pod {pod_name}: {e}", file=sys.stderr)
        return ("N/A", "N/A")


def collect_individual_pod_executions(
    task_name,
    steps,
    days=7,
    hours=0,
    lookback_seconds=None,
    parallel_clusters=None,
    debug=False,
    current_resources=None,
    pll_queries=2,
):
    """Collect individual pod execution data from all clusters.

    Each pod execution represents one pod run. For each pod, we get:
    - Max memory usage during pod lifetime
    - Max CPU usage during pod lifetime
    - Pod start timestamp (or first metric timestamp)
    - Current requests/limits from YAML when current_resources is provided

    Pods are listed once per cluster (not once per step). Returns
    (executions_list, collection_stats).

    Args:
        task_name: Task name
        steps: List of step names (without 'step-' prefix)
        days: Whole days in the lookback window
        hours: Additional hours in the lookback window (clubbed with days)
        lookback_seconds: Optional explicit lookback; overrides days/hours when set
        parallel_clusters: Number of parallel workers (None for serial)
        debug: Enable debug output including capped skip-reason samples
        current_resources: Optional dict step_name ->
            {requests: {memory, cpu}, limits: {memory, cpu}}
                          from extract_task_info(); used to add mem_requests_k8s, etc. to each
                          execution.
        pll_queries: Number of Prometheus queries to run in parallel per pod (1-4).

    Returns:
        Tuple (list of execution dicts, collection_stats dict)
    """
    script_dir = Path(__file__).parent
    wrapper_path = script_dir / "wrapper_for_promql_for_all_clusters.sh"

    empty_stats = {
        **_empty_collection_counters(),
        "per_cluster": {},
        "debug_samples": [],
        "lookback_seconds": 0,
        "lookback_label": "0",
    }

    if not wrapper_path.exists():
        return [], empty_stats

    if lookback_seconds is None:
        lookback_seconds = resolve_lookback_seconds(days, hours)
    lookback_seconds = int(lookback_seconds)
    range_str = format_promql_duration(lookback_seconds)
    lookback_label = format_lookback_label(days, hours)
    # get_component_for_pod.py still expects integer days
    component_days = max(1, (lookback_seconds + 86399) // 86400)

    all_executions = []
    collection_stats = {
        **_empty_collection_counters(),
        "per_cluster": {},
        "debug_samples": [],
        "lookback_seconds": lookback_seconds,
        "lookback_label": lookback_label,
    }
    samples_lock = Lock()

    def add_debug_sample(reason, cluster_name, pod_name="", namespace="", step="", detail=""):
        if not debug:
            return
        with samples_lock:
            if len(collection_stats["debug_samples"]) >= DEBUG_SKIP_SAMPLE_LIMIT:
                return
            collection_stats["debug_samples"].append(
                {
                    "reason": reason,
                    "cluster": cluster_name,
                    "pod": pod_name,
                    "namespace": namespace,
                    "step": step,
                    "detail": detail,
                }
            )

    # Extract cluster list
    clusters_raw = extract_cluster_list(wrapper_path)
    if not clusters_raw:
        return [], collection_stats

    clusters = list(dict.fromkeys([c.strip() for c in clusters_raw if c and c.strip()]))

    def process_cluster_for_detailed_data(cluster_ctx):
        """Process a single cluster to get detailed pod execution data."""
        cluster_stats = _empty_collection_counters()
        cluster_name = get_cluster_display_name(cluster_ctx)
        try:
            original_kubeconfig = os.environ.get("KUBECONFIG", os.path.expanduser("~/.kube/config"))
            temp_kubeconfig = (
                script_dir
                / f".kubeconfig_detailed_{cluster_ctx.replace('/', '_').replace(':', '_')}"
            )

            try:
                if os.path.exists(original_kubeconfig):
                    shutil.copy2(original_kubeconfig, temp_kubeconfig)

                env = os.environ.copy()
                env["KUBECONFIG"] = str(temp_kubeconfig)

                subprocess.run(
                    ["kubectl", "config", "use-context", cluster_ctx],
                    capture_output=True,
                    env=env,
                    timeout=10,
                )

                token_result = subprocess.run(
                    ["oc", "whoami", "--show-token"],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=10,
                )
                if token_result.returncode != 0:
                    cluster_stats["list_failures"] += 1
                    add_debug_sample(
                        "auth_failure",
                        cluster_name,
                        detail="oc whoami --show-token failed",
                    )
                    return [], cluster_stats

                token = token_result.stdout.strip()

                prom_result = subprocess.run(
                    [
                        "oc",
                        "-n",
                        "openshift-monitoring",
                        "get",
                        "route",
                        "prometheus-k8s",
                        "--no-headers",
                        "-o",
                        "custom-columns=HOST:.spec.host",
                    ],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=10,
                )
                if prom_result.returncode != 0:
                    cluster_stats["list_failures"] += 1
                    add_debug_sample(
                        "prometheus_route_failure",
                        cluster_name,
                        detail="failed to get prometheus-k8s route",
                    )
                    return [], cluster_stats

                prom_host = prom_result.stdout.strip()
                if not prom_host:
                    cluster_stats["list_failures"] += 1
                    add_debug_sample(
                        "prometheus_route_failure",
                        cluster_name,
                        detail="empty prometheus host",
                    )
                    return [], cluster_stats

                end_time = int(time.time())
                start_time = end_time - lookback_seconds

                cluster_executions = []

                # List pods once per cluster/task (not once per step).
                list_pods_script = script_dir / "list_pods_for_a_particular_task.py"
                if not list_pods_script.exists():
                    cluster_stats["list_failures"] += 1
                    add_debug_sample(
                        "list_script_missing",
                        cluster_name,
                        detail=str(list_pods_script),
                    )
                    return [], cluster_stats

                list_result = subprocess.run(
                    [
                        sys.executable,
                        str(list_pods_script),
                        token,
                        prom_host,
                        task_name,
                        str(end_time),
                        str(lookback_seconds),
                    ],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=60,
                )

                if list_result.returncode != 0:
                    cluster_stats["list_failures"] += 1
                    add_debug_sample(
                        "list_pods_failure",
                        cluster_name,
                        detail=f"rc={list_result.returncode}",
                    )
                    return [], cluster_stats

                try:
                    pods_data = json.loads(list_result.stdout)
                    pods = []
                    seen = set()
                    if "data" in pods_data and "result" in pods_data["data"]:
                        for result in pods_data["data"]["result"]:
                            pod_name = result.get("metric", {}).get("pod", "")
                            namespace = result.get("metric", {}).get("namespace", "")
                            if not pod_name:
                                continue
                            key = (pod_name, namespace)
                            if key in seen:
                                continue
                            seen.add(key)
                            pods.append(key)
                except (json.JSONDecodeError, KeyError) as e:
                    cluster_stats["list_failures"] += 1
                    add_debug_sample(
                        "list_pods_parse_error",
                        cluster_name,
                        detail=str(e),
                    )
                    return [], cluster_stats

                cluster_stats["pods_listed"] = len(pods)
                if debug:
                    print(
                        f"DEBUG: Found {len(pods)} unique pods for task {task_name} "
                        f"in cluster {cluster_name} (lookback={lookback_label})",
                        file=sys.stderr,
                    )

                # Early exit: no pods on this cluster.
                if not pods:
                    return [], cluster_stats

                query_script = script_dir / "query_prometheus_range.py"
                if not query_script.exists():
                    cluster_stats["query_failures"] += len(pods) * len(steps)
                    add_debug_sample(
                        "query_script_missing",
                        cluster_name,
                        detail=str(query_script),
                    )
                    return [], cluster_stats

                for step in steps:
                    step_name = f"step-{step}" if not step.startswith("step-") else step

                    for pod_name, namespace in pods:
                        cluster_stats["pods_queried"] += 1
                        ns_filter = 'namespace=~".*-tenant"'
                        labels = f'container="{step_name}",pod="{pod_name}",{ns_filter}'
                        # f-string: {{{labels}}} -> {labels=...}; five braces
                        # would emit invalid PromQL {{...}}.
                        mem_query = (
                            f"max_over_time("
                            f"container_memory_working_set_bytes"
                            f"{{{labels}}}[{range_str}])"
                        )
                        cpu_query = (
                            f"max_over_time(rate("
                            f"container_cpu_usage_seconds_total"
                            f"{{{labels}}}[5m])"
                            f"[{range_str}:5m])"
                        )
                        io_read_query = (
                            f"max_over_time(rate("
                            f"container_fs_reads_bytes_total"
                            f"{{{labels}}}[5m])"
                            f"[{range_str}:5m])"
                        )
                        io_write_query = (
                            f"max_over_time(rate("
                            f"container_fs_writes_bytes_total"
                            f"{{{labels}}}[5m])"
                            f"[{range_str}:5m])"
                        )

                        def _stderr_snip(proc_or_exc, limit=280):
                            if isinstance(proc_or_exc, Exception):
                                return f"{type(proc_or_exc).__name__}: {proc_or_exc}"[:limit]
                            err = (getattr(proc_or_exc, "stderr", None) or "").strip()
                            if not err:
                                return ""
                            # Prefer the last non-empty line (often the real error).
                            lines = [ln.strip() for ln in err.splitlines() if ln.strip()]
                            snip = lines[-1] if lines else err
                            return snip.replace("\n", " ")[:limit]

                        def _run_query(metric_cmd_pair):
                            """Run one PromQL helper; retry transient HTTP/connection failures."""
                            metric_name, cmd = metric_cmd_pair
                            last = None
                            for attempt in range(3):
                                try:
                                    proc = subprocess.run(
                                        cmd,
                                        capture_output=True,
                                        text=True,
                                        env=env,
                                        timeout=60,
                                    )
                                    last = proc
                                    if proc.returncode == 0:
                                        return metric_name, proc
                                except Exception as exc:
                                    last = exc
                                if attempt < 2:
                                    time.sleep(0.4 * (attempt + 1))
                            return metric_name, last

                        query_cmds = [
                            (
                                "mem",
                                [
                                    sys.executable,
                                    str(query_script),
                                    token,
                                    prom_host,
                                    mem_query,
                                    str(start_time),
                                    str(end_time),
                                ],
                            ),
                            (
                                "cpu",
                                [
                                    sys.executable,
                                    str(query_script),
                                    token,
                                    prom_host,
                                    cpu_query,
                                    str(start_time),
                                    str(end_time),
                                ],
                            ),
                            (
                                "io_read",
                                [
                                    sys.executable,
                                    str(query_script),
                                    token,
                                    prom_host,
                                    io_read_query,
                                    str(start_time),
                                    str(end_time),
                                ],
                            ),
                            (
                                "io_write",
                                [
                                    sys.executable,
                                    str(query_script),
                                    token,
                                    prom_host,
                                    io_write_query,
                                    str(start_time),
                                    str(end_time),
                                ],
                            ),
                        ]

                        query_results = {}
                        _pll = max(1, min(4, pll_queries))
                        with ThreadPoolExecutor(max_workers=_pll) as pod_executor:
                            for metric_name, proc_or_exc in pod_executor.map(
                                _run_query, query_cmds
                            ):
                                query_results[metric_name] = proc_or_exc

                        mem_result = query_results.get("mem")
                        cpu_result = query_results.get("cpu")
                        io_read_result = query_results.get("io_read")
                        io_write_result = query_results.get("io_write")

                        mem_ok = (
                            not isinstance(mem_result, Exception)
                            and getattr(mem_result, "returncode", 1) == 0
                            and (mem_result.stdout or "").strip()
                        )
                        # Memory is required; CPU/IO failures are non-fatal (cpu_cores=0).
                        if not mem_ok:
                            cluster_stats["query_failures"] += 1
                            detail_parts = []
                            if isinstance(mem_result, Exception):
                                detail_parts.append(f"mem={type(mem_result).__name__}")
                            else:
                                detail_parts.append(
                                    f"mem_rc={getattr(mem_result, 'returncode', '?')}"
                                )
                            snip = _stderr_snip(mem_result)
                            if snip:
                                detail_parts.append(f"mem_err={snip}")
                            if isinstance(cpu_result, Exception):
                                detail_parts.append(f"cpu={type(cpu_result).__name__}")
                            elif getattr(cpu_result, "returncode", 0) != 0:
                                detail_parts.append(f"cpu_rc={cpu_result.returncode}")
                                cpu_snip = _stderr_snip(cpu_result)
                                if cpu_snip:
                                    detail_parts.append(f"cpu_err={cpu_snip}")
                            add_debug_sample(
                                "query_failure",
                                cluster_name,
                                pod_name=pod_name,
                                namespace=namespace,
                                step=step,
                                detail=", ".join(detail_parts) or "mem query failed",
                            )
                            continue

                        try:
                            mem_data = json.loads(mem_result.stdout)
                            if (
                                not isinstance(cpu_result, Exception)
                                and getattr(cpu_result, "returncode", 1) == 0
                                and (cpu_result.stdout or "").strip()
                            ):
                                cpu_data = json.loads(cpu_result.stdout)
                            else:
                                cpu_data = {"data": {"result": []}}
                            io_read_data = (
                                json.loads(io_read_result.stdout)
                                if (
                                    not isinstance(io_read_result, Exception)
                                    and io_read_result.returncode == 0
                                    and io_read_result.stdout.strip()
                                )
                                else {}
                            )
                            io_write_data = (
                                json.loads(io_write_result.stdout)
                                if (
                                    not isinstance(io_write_result, Exception)
                                    and io_write_result.returncode == 0
                                    and io_write_result.stdout.strip()
                                )
                                else {}
                            )

                            mem_max = 0
                            cpu_max = 0
                            io_read_max_bytes_s = 0
                            io_write_max_bytes_s = 0
                            first_timestamp = None

                            mem_series = mem_data.get("data", {}).get("result", [])
                            cpu_series = cpu_data.get("data", {}).get("result", [])
                            io_read_series = io_read_data.get("data", {}).get("result", [])
                            io_write_series = io_write_data.get("data", {}).get("result", [])

                            matched_mem_series = False
                            if mem_series:
                                for series in mem_series:
                                    metric = series.get("metric", {})
                                    if metric.get("pod") == pod_name:
                                        matched_mem_series = True
                                        values = series.get("values", [])
                                        if values:
                                            if first_timestamp is None:
                                                first_timestamp = float(values[0][0])
                                            for _ts, val in values:
                                                mem_bytes = float(val) if val else 0
                                                if mem_bytes > mem_max:
                                                    mem_max = mem_bytes

                            if cpu_series:
                                for series in cpu_series:
                                    metric = series.get("metric", {})
                                    if metric.get("pod") == pod_name:
                                        values = series.get("values", [])
                                        for _ts, val in values:
                                            cpu_val = float(val) if val else 0
                                            if cpu_val > cpu_max:
                                                cpu_max = cpu_val

                            for series in io_read_series:
                                if series.get("metric", {}).get("pod") == pod_name:
                                    for _ts, val in series.get("values", []):
                                        v = float(val) if val else 0
                                        if v > io_read_max_bytes_s:
                                            io_read_max_bytes_s = v

                            for series in io_write_series:
                                if series.get("metric", {}).get("pod") == pod_name:
                                    for _ts, val in series.get("values", []):
                                        v = float(val) if val else 0
                                        if v > io_write_max_bytes_s:
                                            io_write_max_bytes_s = v

                            if mem_max == 0 and first_timestamp is None:
                                cluster_stats["empty_metrics"] += 1
                                if mem_series and not matched_mem_series:
                                    reason = "pod_label_mismatch"
                                    detail = (
                                        f"mem_series={len(mem_series)} but none matched "
                                        f"pod={pod_name} container={step_name}"
                                    )
                                elif mem_series and matched_mem_series:
                                    reason = "empty_values"
                                    detail = f"matched series had no values container={step_name}"
                                else:
                                    reason = "empty_metrics"
                                    detail = f"no container_memory series for container={step_name}"
                                add_debug_sample(
                                    reason,
                                    cluster_name,
                                    pod_name=pod_name,
                                    namespace=namespace,
                                    step=step,
                                    detail=detail,
                                )
                                continue

                            component, application = extract_component_from_pod(
                                pod_name,
                                namespace,
                                token,
                                prom_host,
                                end_time,
                                component_days,
                            )

                            mem_mb = mem_max / (1024 * 1024)
                            io_read_mbps = round(io_read_max_bytes_s / (1024 * 1024), 3)
                            io_write_mbps = round(io_write_max_bytes_s / (1024 * 1024), 3)

                            if first_timestamp:
                                exec_timestamp = datetime.fromtimestamp(first_timestamp).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                            else:
                                exec_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            res = (current_resources or {}).get(step, {}) or {}
                            req = res.get("requests") or {}
                            lim = res.get("limits") or {}
                            mem_req_k8s = req.get("memory") if req.get("memory") else "N/A"
                            cpu_req_k8s = req.get("cpu") if req.get("cpu") else "N/A"
                            mem_lim_k8s = lim.get("memory") if lim.get("memory") else "N/A"
                            cpu_lim_k8s = lim.get("cpu") if lim.get("cpu") else "N/A"

                            cluster_executions.append(
                                {
                                    "task": task_name,
                                    "step": step,
                                    "component": component,
                                    "application": application,
                                    "cluster": cluster_name,
                                    "pod": pod_name,
                                    "namespace": namespace,
                                    "timestamp": exec_timestamp,
                                    "memory_mb": round(mem_mb, 2),
                                    "cpu_cores": round(cpu_max, 4),
                                    "io_read_mbps": io_read_mbps,
                                    "io_write_mbps": io_write_mbps,
                                    "mem_requests_k8s": mem_req_k8s,
                                    "cpu_requests_k8s": cpu_req_k8s,
                                    "mem_limits_k8s": mem_lim_k8s,
                                    "cpu_limits_k8s": cpu_lim_k8s,
                                }
                            )
                            cluster_stats["pods_kept"] += 1

                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            cluster_stats["parse_errors"] += 1
                            add_debug_sample(
                                "parse_error",
                                cluster_name,
                                pod_name=pod_name,
                                namespace=namespace,
                                step=step,
                                detail=str(e),
                            )
                            if debug:
                                print(
                                    f"DEBUG: Error processing pod {pod_name}: {e}",
                                    file=sys.stderr,
                                )
                            continue

                return cluster_executions, cluster_stats

            finally:
                if temp_kubeconfig.exists():
                    temp_kubeconfig.unlink()

        except Exception as e:
            if debug:
                print(f"DEBUG: Error processing cluster {cluster_ctx}: {e}", file=sys.stderr)
            cluster_stats["list_failures"] += 1
            add_debug_sample(
                "cluster_error",
                cluster_name,
                detail=str(e),
            )
            return [], cluster_stats

    total_clusters = len(clusters)
    progress_data = {
        "completed": [],
        "pods_listed": 0,
        "pods_queried": 0,
        "pods_kept": 0,
    }
    progress_lock = Lock()
    spinner_stop = Event()
    spinner_thread = Thread(
        target=_spinner_thread,
        args=(spinner_stop, progress_data, progress_lock, total_clusters),
        daemon=True,
    )
    spinner_thread.start()
    try:
        if parallel_clusters and parallel_clusters > 0:
            with ThreadPoolExecutor(max_workers=parallel_clusters) as executor:
                futures = {
                    executor.submit(process_cluster_for_detailed_data, cluster): cluster
                    for cluster in clusters
                }
                for future in as_completed(futures):
                    cluster_ctx = futures[future]
                    executions, cluster_stats = future.result()
                    display = get_cluster_display_name(cluster_ctx)
                    with progress_lock:
                        if display not in progress_data["completed"]:
                            progress_data["completed"].append(display)
                        progress_data["pods_listed"] += cluster_stats.get("pods_listed", 0)
                        progress_data["pods_queried"] += cluster_stats.get("pods_queried", 0)
                        progress_data["pods_kept"] += cluster_stats.get("pods_kept", 0)
                    collection_stats["per_cluster"][display] = cluster_stats
                    _merge_counters(collection_stats, cluster_stats)
                    all_executions.extend(executions)
        else:
            for cluster in clusters:
                executions, cluster_stats = process_cluster_for_detailed_data(cluster)
                display = get_cluster_display_name(cluster)
                with progress_lock:
                    if display not in progress_data["completed"]:
                        progress_data["completed"].append(display)
                    progress_data["pods_listed"] += cluster_stats.get("pods_listed", 0)
                    progress_data["pods_queried"] += cluster_stats.get("pods_queried", 0)
                    progress_data["pods_kept"] += cluster_stats.get("pods_kept", 0)
                collection_stats["per_cluster"][display] = cluster_stats
                _merge_counters(collection_stats, cluster_stats)
                all_executions.extend(executions)
    finally:
        spinner_stop.set()
        spinner_thread.join(timeout=1.0)
        print("=" * 80, file=sys.stderr)
        print(
            "Getting information from all clusters: (Completed No. of clusters: 100%)",
            file=sys.stderr,
        )
        print("=" * 80, file=sys.stderr)
        print(
            "Collection summary: "
            f"listed={collection_stats['pods_listed']} "
            f"queried={collection_stats['pods_queried']} "
            f"kept={collection_stats['pods_kept']} "
            f"query_failures={collection_stats['query_failures']} "
            f"empty_metrics={collection_stats['empty_metrics']} "
            f"parse_errors={collection_stats['parse_errors']} "
            f"list_failures={collection_stats['list_failures']}",
            file=sys.stderr,
        )
        if collection_stats["per_cluster"]:
            print("Per-cluster collection stats:", file=sys.stderr)
            for cl in sorted(collection_stats["per_cluster"]):
                cs = collection_stats["per_cluster"][cl]
                print(
                    f"  {cl}: listed={cs['pods_listed']} queried={cs['pods_queried']} "
                    f"kept={cs['pods_kept']} query_failures={cs['query_failures']} "
                    f"empty_metrics={cs['empty_metrics']}",
                    file=sys.stderr,
                )
        if debug and collection_stats["debug_samples"]:
            print(
                f"DEBUG: Skip samples "
                f"(showing {len(collection_stats['debug_samples'])}/"
                f"{DEBUG_SKIP_SAMPLE_LIMIT} capped):",
                file=sys.stderr,
            )
            for sample in collection_stats["debug_samples"]:
                print(
                    f"  [{sample['reason']}] cluster={sample['cluster']} "
                    f"step={sample['step']} ns={sample['namespace']} "
                    f"pod={sample['pod']} detail={sample['detail']}",
                    file=sys.stderr,
                )
        if all_executions:
            print(
                f"Collected data from {len(progress_data['completed'])} cluster(s), total pod"
                f" executions: {len(all_executions)}",
                file=sys.stderr,
            )
        print(file=sys.stderr)

    return all_executions, collection_stats


def generate_diff_patch(original_yaml, updated_yaml, file_path_or_url):
    """Generate a diff/patch file for remote YAML files."""
    script_dir = Path(__file__).parent

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as orig_file:
        yaml.dump(original_yaml, orig_file, default_flow_style=False, sort_keys=False, width=120)
        orig_path = orig_file.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as upd_file:
        yaml.dump(updated_yaml, upd_file, default_flow_style=False, sort_keys=False, width=120)
        upd_path = upd_file.name

    try:
        # Generate diff using diff command
        result = subprocess.run(["diff", "-u", orig_path, upd_path], capture_output=True, text=True)

        # Extract filename from URL for patch file naming
        url_parts = file_path_or_url.split("/")
        filename = url_parts[-1] if url_parts else "buildah.yaml"
        # Remove .yaml extension and add timestamp
        base_name = filename.replace(".yaml", "").replace(".yml", "")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        patch_file = script_dir / f"{base_name}_{timestamp}.patch"

        # Extract relative path from URL for patch file context
        # e.g., "task/buildah/0.7/buildah.yaml" from the full URL
        relative_path = None
        if "/task/" in file_path_or_url:
            idx = file_path_or_url.find("/task/")
            relative_path = file_path_or_url[idx + 1 :] if idx >= 0 else filename

        # Write patch file with header
        with open(patch_file, "w") as f:
            f.write(f"# Resource limits patch for: {file_path_or_url}\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# File: {relative_path or filename}\n")
            f.write("#\n")
            f.write("# To apply this patch:\n")
            f.write("#   1. Download the original file from the URL above\n")
            f.write(f"#   2. Apply: patch <original_file> < {patch_file.name}\n")
            f.write("#   3. Or manually apply the changes shown below\n")
            f.write("#\n")
            # Replace temp file paths in diff with relative path
            diff_output = result.stdout
            if relative_path:
                # Replace the temp file paths with the relative path
                lines = diff_output.split("\n")
                if len(lines) >= 2:
                    f.write(f"--- a/{relative_path}\n")
                    f.write(f"+++ b/{relative_path}\n")
                    # Skip the first two lines (temp file paths) and write the rest
                    f.write("\n".join(lines[2:]))
                else:
                    f.write(diff_output)
            else:
                f.write(diff_output)

        print(f"\nGenerated patch file: {patch_file}", file=sys.stderr)
        print(f"Apply with: patch -p1 < {patch_file.name}", file=sys.stderr)

        return patch_file
    finally:
        # Clean up temp files
        os.unlink(orig_path)
        os.unlink(upd_path)


def update_yaml_file(yaml_path, recommendations, original_yaml, file_path_or_url=None, debug=False):
    """Update YAML file with recommended resource limits, preserving original formatting."""
    updated = False

    # Build a map of step names to their resource recommendations
    step_updates = {}
    for rec in recommendations:
        if rec is None:
            continue

        step_name_csv = rec["step_name"]  # e.g., "step-build"
        mem_k8s = rec["mem_recommended_k8s"]
        cpu_k8s = rec["cpu_recommended_k8s"]

        # Convert "step-build" to "build" for YAML matching
        step_name_yaml = normalize_step_name_for_compare(step_name_csv)
        step_updates[step_name_yaml] = {"memory": mem_k8s, "cpu": cpu_k8s}

    if not step_updates:
        return False

    if debug:
        print(f"DEBUG: Looking for steps: {list(step_updates.keys())}", file=sys.stderr)

    # If remote URL, use the old method (generate patch)
    if file_path_or_url and (
        file_path_or_url.startswith("http://") or file_path_or_url.startswith("https://")
    ):
        updated_yaml = yaml.safe_load(yaml.dump(original_yaml))  # Deep copy

        for step_name_yaml, resources in step_updates.items():
            steps = updated_yaml.get("spec", {}).get("steps", [])
            for step in steps:
                if step.get("name") == step_name_yaml:
                    if "computeResources" not in step:
                        step["computeResources"] = {}
                    if "limits" not in step["computeResources"]:
                        step["computeResources"]["limits"] = {}
                    if "requests" not in step["computeResources"]:
                        step["computeResources"]["requests"] = {}

                    step["computeResources"]["limits"]["memory"] = resources["memory"]
                    step["computeResources"]["limits"]["cpu"] = resources["cpu"]
                    step["computeResources"]["requests"]["memory"] = resources["memory"]
                    step["computeResources"]["requests"]["cpu"] = resources["cpu"]
                    updated = True
                    print(
                        f"Updated step '{step_name_yaml}': memory={resources['memory']},"
                        f" cpu={resources['cpu']}",
                        file=sys.stderr,
                    )
                    break

        if updated:
            generate_diff_patch(original_yaml, updated_yaml, file_path_or_url)
        return updated

    # Note: debug parameter is not used for remote URLs (patch generation)

    # For local files, update by modifying text directly to preserve formatting
    if yaml_path and not yaml_path.startswith("http"):
        # Read the file as text
        with open(yaml_path) as f:
            lines = f.readlines()

        # First pass: collect all step positions (process in reverse to avoid index shifting)
        step_positions = []
        i = 0
        in_steps_section = False
        while i < len(lines):
            line = lines[i]

            # Track if we're in the steps section
            if re.match(r"^\s+steps:\s*$", line):
                in_steps_section = True
                if debug:
                    print(f"DEBUG: Entered steps section at line {i + 1}", file=sys.stderr)
            elif in_steps_section:
                line_indent = len(line) - len(line.lstrip())
                if (
                    line_indent <= 2
                    and line.strip()
                    and not line.strip().startswith("-")
                    and not line.strip().startswith("#")
                ) and ("workspaces:" in line or "results:" in line or "volumes:" in line):
                    in_steps_section = False
                    if debug:
                        print(
                            f"DEBUG: Left steps section at line {i + 1}: {line.strip()}",
                            file=sys.stderr,
                        )

            # Look for step names
            if in_steps_section:
                line_indent = len(line) - len(line.lstrip())
                is_step = False
                step_name_match = None

                # Pattern 1: "    - name: stepname" or "  - name: stepname" (2 or 4 spaces: 2 for
                # steps:, 0-2 for list item)
                if re.match(r"^\s+-\s+name:\s+", line) and line_indent in (2, 4):
                    is_step = True
                    step_name_match = re.search(r"name:\s+(.+)$", line)
                # Pattern 2: "    name: stepname" (not starting with "-")
                elif (
                    re.match(r"^\s+name:\s+", line)
                    and line_indent in (2, 4)
                    and not line.strip().startswith("-")
                ):
                    for lookback in range(1, min(11, i + 1)):
                        prev_line = lines[i - lookback]
                        prev_stripped = prev_line.lstrip()
                        prev_indent = len(prev_line) - len(prev_stripped)
                        if not prev_stripped:
                            continue
                        if prev_indent == 4 and prev_stripped.endswith(":"):
                            is_step = False
                            break
                        if (
                            prev_indent == 2
                            and prev_stripped.startswith("-")
                            and (
                                "image:" in prev_line
                                or prev_stripped.startswith("- image:")
                                or prev_stripped.startswith("- name:")
                            )
                        ):
                            is_step = True
                            break
                        if prev_indent <= 2 and (
                            prev_stripped.startswith("steps:")
                            or (
                                prev_stripped
                                and not prev_stripped.startswith("-")
                                and ":" in prev_stripped
                            )
                        ):
                            break
                    if is_step:
                        step_name_match = re.search(r"name:\s+(.+)$", line)

                if step_name_match and is_step:
                    step_name = step_name_match.group(1).strip()
                    if step_name in step_updates:
                        step_positions.append((i, step_name))

            i += 1

        # Second pass: process steps in reverse order (bottom to top) to avoid index shifting
        step_positions.sort(reverse=True)  # Process from bottom to top

        # Process each step
        for step_line_idx, step_name in step_positions:
            # Find the current line number for this step (may have shifted due to previous
            # insertions)
            # Search more broadly - start from a reasonable position and search forward/backward
            current_step_line = None

            # Calculate approximate shift: each step processed before this one (in reverse order)
            # may have added ~7 lines
            steps_before = sum(1 for idx, name in step_positions if idx > step_line_idx)
            estimated_shift = steps_before * 7  # Approximate lines added per step
            search_center = step_line_idx + estimated_shift

            # Search in a wider range
            search_start = max(0, search_center - 50)
            search_end = min(len(lines), search_center + 100)

            # First try near the estimated position
            for search_idx in range(search_start, search_end):
                if search_idx < len(lines):
                    search_line = lines[search_idx]
                    line_indent = len(search_line) - len(search_line.lstrip())
                    # Check Pattern 1: "    - name: stepname" or "  - name: stepname" (2 or 4
                    # spaces: 2 for steps:, 0-2 for list item)
                    if re.match(r"^\s+-\s+name:\s+", search_line) and line_indent in (2, 4):
                        name_match = re.search(r"name:\s+(.+)$", search_line)
                        if name_match and name_match.group(1).strip() == step_name:
                            current_step_line = search_idx
                            break
                    # Check Pattern 2: "    name: stepname" (not starting with "-")
                    elif (
                        re.match(r"^\s+name:\s+", search_line)
                        and line_indent in (2, 4)
                        and not search_line.strip().startswith("-")
                    ):
                        # Verify it's a step name by checking previous lines
                        is_step_name = False
                        for lookback in range(1, min(6, search_idx + 1)):
                            prev_line = lines[search_idx - lookback]
                            prev_stripped = prev_line.lstrip()
                            prev_indent = len(prev_line) - len(prev_stripped)
                            if (
                                prev_indent == 2
                                and prev_stripped.startswith("-")
                                and "image:" in prev_line
                            ):
                                is_step_name = True
                                break
                            if prev_indent <= 2:
                                break
                        if is_step_name:
                            name_match = re.search(r"name:\s+(.+)$", search_line)
                            if name_match and name_match.group(1).strip() == step_name:
                                current_step_line = search_idx
                                break

            # If not found, do a broader search through the entire steps section
            if current_step_line is None:
                # Find steps: line first
                steps_section_start = None
                for idx, line in enumerate(lines):
                    if re.match(r"^\s+steps:\s*$", line):
                        steps_section_start = idx
                        break

                if steps_section_start is not None:
                    # Search from steps: to end of file (or next top-level section)
                    for search_idx in range(steps_section_start + 1, len(lines)):
                        search_line = lines[search_idx]
                        line_indent = len(search_line) - len(search_line.lstrip())
                        # Stop if we've left steps section
                        if (
                            line_indent <= 2
                            and search_line.strip()
                            and not search_line.strip().startswith("-")
                            and not search_line.strip().startswith("#")
                        ) and (
                            "workspaces:" in search_line
                            or "results:" in search_line
                            or "volumes:" in search_line
                        ):
                            break

                        # Check for step name (2 or 4 spaces: 2 for steps:, 0-2 for list item)
                        if re.match(r"^\s+-\s+name:\s+", search_line) and line_indent in (2, 4):
                            name_match = re.search(r"name:\s+(.+)$", search_line)
                            if name_match and name_match.group(1).strip() == step_name:
                                current_step_line = search_idx
                                break
                        elif (
                            re.match(r"^\s+name:\s+", search_line)
                            and line_indent in (2, 4)
                            and not search_line.strip().startswith("-")
                        ):
                            name_match = re.search(r"name:\s+(.+)$", search_line)
                            if name_match and name_match.group(1).strip() == step_name:
                                # Verify it's a step
                                for lookback in range(1, min(6, search_idx + 1)):
                                    prev_line = lines[search_idx - lookback]
                                    if (
                                        len(prev_line) - len(prev_line.lstrip()) == 2
                                        and prev_line.lstrip().startswith("-")
                                        and "image:" in prev_line
                                    ):
                                        current_step_line = search_idx
                                        break
                                if current_step_line is not None:
                                    break

            if current_step_line is None:
                if debug:
                    print(
                        f"WARNING: Could not find step '{step_name}' (original line"
                        f" {step_line_idx + 1}), skipping",
                        file=sys.stderr,
                    )
                continue

            line = lines[current_step_line]
            if debug:
                print(
                    f"DEBUG: Processing step '{step_name}' at line {current_step_line + 1}",
                    file=sys.stderr,
                )

            # Check if this step needs updating
            if step_name in step_updates:
                resources = step_updates[step_name]
                if debug:
                    print(
                        f"DEBUG: Processing step '{step_name}', looking for computeResources...",
                        file=sys.stderr,
                    )

                # Look ahead for computeResources section within this step
                j = current_step_line + 1
                step_line = lines[current_step_line]
                step_indent = len(step_line) - len(step_line.lstrip())

                # Find the actual indent for step content fields (like image:, computeResources:)
                # Step content should be indented 4 spaces (same as image:, env:, etc.)
                step_content_indent = step_indent + 2  # Default: 2 spaces more than step name
                # Look for image: or other step fields to determine correct indent
                for k in range(current_step_line, min(current_step_line + 10, len(lines))):
                    check_line = lines[k]
                    check_indent = len(check_line) - len(check_line.lstrip())
                    if "image:" in check_line or "env:" in check_line or "script:" in check_line:
                        step_content_indent = check_indent
                        break

                in_compute_resources = False
                in_limits = False
                in_requests = False
                limits_indent = None
                requests_indent = None
                memory_updated_limits = False
                cpu_updated_limits = False
                memory_updated_requests = False
                cpu_updated_requests = False
                compute_resources_found = False
                compute_resources_insert_pos = None
                compute_resources_indent = None  # Will be set when we find computeResources

                while j < len(lines):
                    next_line = lines[j]
                    next_stripped = next_line.lstrip()
                    next_indent = len(next_line) - len(next_stripped)

                    # Stop if we've moved to the next step (same or less indent with "- name:" or
                    # "name:")
                    if next_indent <= step_indent and (
                        next_stripped.startswith("- name:") or next_stripped.startswith("name:")
                    ):
                        break

                    # Stop if we've moved to a different top-level section
                    if next_indent < step_indent:
                        break

                    # Find computeResources section (at same indent level as step content fields
                    # like image:)
                    # Allow some flexibility in indent to handle formatting variations
                    if "computeResources:" in next_line and (
                        next_indent == step_content_indent
                        or (step_content_indent <= 4 and next_indent >= 2 and next_indent <= 6)
                    ):
                        in_compute_resources = True
                        compute_resources_found = True
                        compute_resources_indent = (
                            next_indent  # Store the actual indent of computeResources
                        )
                        if debug:
                            print(
                                f"DEBUG: Found computeResources at line"
                                f" {j + 1}, indent={next_indent}"
                                f" (step_content_indent="
                                f"{step_content_indent})",
                                file=sys.stderr,
                            )
                        j += 1
                        continue

                    # Track where to insert computeResources if not found (after name: and image:,
                    # before other fields)
                    if not compute_resources_found and compute_resources_insert_pos is None:
                        # Stop if we've left the step (hit next step marker or back to step list
                        # level)
                        if next_indent <= step_indent:
                            # We've left the step, insert before leaving (at the end of current
                            # step)
                            # Find the last field of current step by going back
                            for k in range(j - 1, max(current_step_line, j - 20), -1):
                                if k < len(lines):
                                    check_line = lines[k]
                                    check_indent = len(check_line) - len(check_line.lstrip())
                                    if (
                                        check_indent == step_content_indent
                                        and check_line.strip()
                                        and not check_line.strip().startswith("#")
                                    ):
                                        compute_resources_insert_pos = k + 1
                                        break
                            if compute_resources_insert_pos is None:
                                # Fallback: insert right before leaving the step
                                compute_resources_insert_pos = j
                            break
                        # Insert after name: or image: (whichever comes last)
                        elif "image:" in next_line or "name:" in next_line:
                            compute_resources_insert_pos = j + 1
                        elif (
                            next_stripped
                            and not next_stripped.startswith("#")
                            and next_indent == step_content_indent
                        ):
                            # We've hit another field at step content level, insert before it
                            compute_resources_insert_pos = j
                            break

                    if not in_compute_resources:
                        j += 1
                        continue

                    # Stop if we've left computeResources section (back to step level or next step)
                    # computeResources is at step_indent, so if we go back to step_indent or less,
                    # we've left it
                    if (
                        next_indent <= step_indent
                        and next_stripped
                        and not next_stripped.startswith("#")
                        and "computeResources" not in next_line
                    ):
                        break

                    # Find limits section (should be indented 2 spaces more than computeResources)
                    # Use compute_resources_indent if available, otherwise fall back to step_indent
                    # + 2
                    expected_limits_indent = (
                        compute_resources_indent + 2
                        if compute_resources_indent is not None
                        else step_indent + 2
                    )
                    if "limits:" in next_line and next_indent == expected_limits_indent:
                        in_limits = True
                        in_requests = False
                        limits_indent = next_indent
                        if debug:
                            print(
                                f"DEBUG: Found limits at line {j + 1}, indent={next_indent}",
                                file=sys.stderr,
                            )
                        j += 1
                        continue

                    # Find requests section (should be indented 2 spaces more than computeResources)
                    # Use compute_resources_indent if available, otherwise fall back to step_indent
                    # + 2
                    expected_requests_indent = (
                        compute_resources_indent + 2
                        if compute_resources_indent is not None
                        else step_indent + 2
                    )
                    if "requests:" in next_line and next_indent == expected_requests_indent:
                        # Before leaving limits section, add missing fields
                        if in_limits and not cpu_updated_limits:
                            indent = " " * (limits_indent + 2)
                            lines.insert(j, f"{indent}cpu: {resources['cpu']}\n")
                            cpu_updated_limits = True
                            updated = True
                            if debug:
                                print(
                                    f"DEBUG: Added cpu to limits at line {j + 1}:"
                                    f" {resources['cpu']}",
                                    file=sys.stderr,
                                )
                            j += 1

                        in_requests = True
                        in_limits = False
                        requests_indent = next_indent
                        if debug:
                            print(
                                f"DEBUG: Found requests at line {j + 1}, indent={next_indent}",
                                file=sys.stderr,
                            )
                        j += 1
                        continue

                    # Update memory and cpu values
                    if in_limits:
                        # Update memory in limits (should be indented 2 spaces more than limits:)
                        if (
                            re.match(r"^\s+memory:\s+", next_line)
                            and next_indent == limits_indent + 2
                        ):
                            indent = next_line[: len(next_line) - len(next_line.lstrip())]
                            lines[j] = f"{indent}memory: {resources['memory']}\n"
                            memory_updated_limits = True
                            updated = True
                            if debug:
                                print(
                                    f"DEBUG: Updated memory in limits at line {j + 1}:"
                                    f" {resources['memory']}",
                                    file=sys.stderr,
                                )

                        # Update cpu in limits (should be indented 2 spaces more than limits:)
                        elif (
                            re.match(r"^\s+cpu:\s+", next_line) and next_indent == limits_indent + 2
                        ):
                            indent = next_line[: len(next_line) - len(next_line.lstrip())]
                            lines[j] = f"{indent}cpu: {resources['cpu']}\n"
                            cpu_updated_limits = True
                            updated = True
                            if debug:
                                print(
                                    f"DEBUG: Updated cpu in limits at line {j + 1}:"
                                    f" {resources['cpu']}",
                                    file=sys.stderr,
                                )

                        # Check if we need to add missing fields (after limits: but before
                        # requests:)
                        elif limits_indent is not None and next_indent > limits_indent:
                            # Still in limits section, check if we need to add fields
                            if not memory_updated_limits and (
                                "requests:" in next_line or next_indent <= limits_indent
                            ):
                                # Insert memory before leaving limits section
                                indent = " " * (limits_indent + 2)
                                lines.insert(j, f"{indent}memory: {resources['memory']}\n")
                                memory_updated_limits = True
                                updated = True
                                j += 1
                            elif not cpu_updated_limits and (
                                "requests:" in next_line or next_indent <= limits_indent
                            ):
                                # Insert cpu before leaving limits section
                                indent = " " * (limits_indent + 2)
                                lines.insert(j, f"{indent}cpu: {resources['cpu']}\n")
                                cpu_updated_limits = True
                                updated = True
                                j += 1

                    elif in_requests:
                        # Update memory in requests (should be indented 2 spaces more than
                        # requests:)
                        if (
                            re.match(r"^\s+memory:\s+", next_line)
                            and next_indent == requests_indent + 2
                        ):
                            indent = next_line[: len(next_line) - len(next_line.lstrip())]
                            lines[j] = f"{indent}memory: {resources['memory']}\n"
                            memory_updated_requests = True
                            updated = True
                            if debug:
                                print(
                                    f"DEBUG: Updated memory in requests at line {j + 1}:"
                                    f" {resources['memory']}",
                                    file=sys.stderr,
                                )

                        # Update cpu in requests (should be indented 2 spaces more than requests:)
                        elif (
                            re.match(r"^\s+cpu:\s+", next_line)
                            and next_indent == requests_indent + 2
                        ):
                            indent = next_line[: len(next_line) - len(next_line.lstrip())]
                            lines[j] = f"{indent}cpu: {resources['cpu']}\n"
                            cpu_updated_requests = True
                            updated = True
                            if debug:
                                print(
                                    f"DEBUG: Updated cpu in requests at line {j + 1}:"
                                    f" {resources['cpu']}",
                                    file=sys.stderr,
                                )

                    # Stop if we've left computeResources section (back to step level)
                    if (
                        in_compute_resources
                        and next_indent <= step_indent
                        and next_stripped
                        and not next_stripped.startswith("#")
                        and "computeResources" not in next_line
                    ):
                        # Add missing fields before leaving
                        if in_limits and not cpu_updated_limits:
                            indent = (
                                " " * (limits_indent + 2)
                                if limits_indent
                                else " " * (step_content_indent + 2)
                            )
                            lines.insert(j, f"{indent}cpu: {resources['cpu']}\n")
                            cpu_updated_limits = True
                            updated = True
                            if debug:
                                print(
                                    f"DEBUG: Added cpu to limits before leaving"
                                    f" computeResources: {resources['cpu']}",
                                    file=sys.stderr,
                                )
                            j += 1
                        if in_requests:
                            if not memory_updated_requests:
                                indent = (
                                    " " * (requests_indent + 2)
                                    if requests_indent
                                    else " " * (step_content_indent + 2)
                                )
                                lines.insert(j, f"{indent}memory: {resources['memory']}\n")
                                memory_updated_requests = True
                                updated = True
                                if debug:
                                    print(
                                        f"DEBUG: Added memory to requests before leaving"
                                        f" computeResources: {resources['memory']}",
                                        file=sys.stderr,
                                    )
                                j += 1
                            if not cpu_updated_requests:
                                indent = (
                                    " " * (requests_indent + 2)
                                    if requests_indent
                                    else " " * (step_content_indent + 2)
                                )
                                lines.insert(j, f"{indent}cpu: {resources['cpu']}\n")
                                cpu_updated_requests = True
                                updated = True
                                if debug:
                                    print(
                                        f"DEBUG: Added cpu to requests before leaving"
                                        f" computeResources: {resources['cpu']}",
                                        file=sys.stderr,
                                    )
                                j += 1
                        break

                    j += 1

                # If computeResources wasn't found, create it
                if not compute_resources_found and compute_resources_insert_pos is not None:
                    indent = " " * step_content_indent
                    compute_resources_block = [
                        f"{indent}computeResources:\n",
                        f"{indent}  limits:\n",
                        f"{indent}    memory: {resources['memory']}\n",
                        f"{indent}    cpu: {resources['cpu']}\n",
                        f"{indent}  requests:\n",
                        f"{indent}    memory: {resources['memory']}\n",
                        f"{indent}    cpu: {resources['cpu']}\n",
                    ]
                    # Insert the block
                    for idx, new_line in enumerate(compute_resources_block):
                        lines.insert(compute_resources_insert_pos + idx, new_line)
                    updated = True
                    if debug:
                        print(
                            f"DEBUG: Created computeResources for step '{step_name}'",
                            file=sys.stderr,
                        )

                if updated:
                    print(
                        f"Updated step '{step_name}': memory={resources['memory']},"
                        f" cpu={resources['cpu']}",
                        file=sys.stderr,
                    )

        # Write back the modified file
        if updated:
            with open(yaml_path, "w") as f:
                f.writelines(lines)
            print(f"\nUpdated YAML file: {yaml_path}", file=sys.stderr)

        return updated

    return False


def main():
    parser = argparse.ArgumentParser(
        description="Analyze resource consumption and provide recommendations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Phase 1: Analysis (default behavior):
    # From piped input:
    ./wrapper_for_promql_for_all_clusters.sh 7 --csv | ./analyze_resource_limits.py

    # From YAML file (auto-runs data collection):
    # Note: --base is IGNORED in Phase 1. All base metrics (max, p95, p90, median) are generated.
    ./analyze_resource_limits.py --file /path/to/buildah.yaml
    ./analyze_resource_limits.py --file https://github.com/.../buildah.yaml

    # Custom safety margin (default: 5%):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --margin 5
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --margin 10
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --margin 20

    # Custom data collection period (default: 7 days):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --days 1
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --days 10
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --days 30

    # Sub-day / combined lookback (--days and --hours are clubbed):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --days 0 --hours 6
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --days 1 --hours 6

    # Combine options (--base is ignored in Phase 1):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --margin 15 --days 10

  Parallel Processing (Phase 1 only):
    # Process multiple clusters concurrently:
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --pll-clusters 3
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --pll-clusters 5

    # Combine with other options:
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --pll-clusters 3 --days 10 --margin 5

  Phase 2: Update (view recommendations):
    # View recommendations for all base metrics (requires --file):
    # Note: --base flag is ignored. All base metrics (max, p95, p90, median) are always shown.
    # YAML files are NOT updated automatically.
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --margin 5

    # Create comparison files for different margins (each margin gets its own file):
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --margin 5
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --margin 10
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --margin 20
    # All three margin files coexist, allowing easy comparison

  Debug and Validation:
    # Enable debug output:
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --debug
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --debug

    # Dry-run: Validate task/steps and check cluster connectivity (Phase 1 only):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --dry-run
    ./analyze_resource_limits.py --file /path/to/buildah.yaml --dry-run --debug

  Complete Examples:
    # Phase 1: Full-featured analysis (generates all base metrics):
    ./analyze_resource_limits.py --file /path/to/buildah.yaml \\
        --margin 15 --days 10 --pll-clusters 4 --debug

    # Two-phase workflow (recommended):
    # Phase 1: Run analysis (generates all base metrics with margin 5%)
    ./analyze_resource_limits.py --file https://github.com/.../buildah.yaml --margin 5 --days 7
    # Phase 2: View recommendations for all base metrics with margin 5%
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --margin 5
    # Phase 2: Create comparison files for margin 10% (separate file, coexists with margin 5% file)
    ./analyze_resource_limits.py --update --file /path/to/buildah.yaml --margin 10
        """,
    )
    parser.add_argument(
        "--file", help="YAML file path or GitHub URL to analyze (auto-runs data collection)"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help=(
            "Phase 2: View recommendations for all base metrics"
            " with specified --margin. Requires --file to specify"
            " which task to update. Creates comparison files for"
            " each margin (if they don't exist). Does not modify"
            " YAML files (user must update manually)."
            " --base flag is ignored."
        ),
    )
    parser.add_argument(
        "--analyze-again",
        "--aa",
        dest="analyze_again",
        action="store_true",
        help="Force re-analysis even when cache exists (only used with --update --file)",
    )
    parser.add_argument(
        "--margin", type=int, default=5, help="Safety margin percentage (default: 5)"
    )
    parser.add_argument(
        "--base",
        type=str,
        choices=["max", "p95", "p90", "median"],
        default="max",
        help=(
            "Base metric for margin calculation: max, p95, p90,"
            " or median (default: max). Ignored in both Phase 1"
            " (analysis) and Phase 2 (--update). All base metrics"
            " are always generated and shown."
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help=(
            "Whole days in the lookback window (default: 7). "
            "Combined with --hours (total = days*24h + hours). Use --days 0 with --hours for "
            "sub-day windows."
        ),
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=0,
        help=(
            "Additional hours in the lookback window (default: 0). "
            "Clubbed with --days, e.g. --days 1 --hours 6 => 30h total."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help=("Enable debug output including pod counts and capped PromQL skip-reason samples"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate task/steps and check cluster connectivity without running data collection",
    )
    parser.add_argument(
        "--pll-clusters",
        type=int,
        metavar="N",
        help=(
            "Enable parallel processing across clusters with"
            " N workers (only during analysis, ignored during"
            " --update)"
        ),
    )
    parser.add_argument(
        "--pll-queries",
        type=int,
        metavar="N",
        default=2,
        help="Number of Prometheus queries to run in parallel per pod (mem/cpu/io_read/io_write). "
        "Default: 2. Max effective value: 4 (one per metric). "
        "Higher values reduce wall-clock time at the cost of more concurrent subprocesses.",
    )

    args = parser.parse_args()

    try:
        lookback_seconds = resolve_lookback_seconds(args.days, args.hours)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    lookback_label = format_lookback_label(args.days, args.hours)
    lookback_days_fraction = lookback_seconds / 86400.0

    # Phase 2: --update (requires --file)
    if args.update:
        if not args.file:
            print(
                "Error: --update requires --file to specify which task to update", file=sys.stderr
            )
            sys.exit(1)

        # Load YAML to get task name and current resources
        yaml_content, yaml_path = fetch_yaml_content(args.file)
        task_name, file_steps, _, current_resources = extract_task_info(yaml_content)

        if not task_name:
            print("Error: Could not extract task name from YAML file", file=sys.stderr)
            sys.exit(1)

        # Find latest analysis date for this task
        analysis_date = find_latest_analysis_date(task_name)
        if not analysis_date:
            print(
                f"Error: No analysis data found for task '{task_name}'. Please run Phase 1"
                f" (analysis) first.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Load analyzed data
        analyzed_data = load_analyzed_data(task_name, analysis_date)
        if not analyzed_data:
            print(
                f"Error: Could not load analyzed data for task '{task_name}' (date:"
                f" {analysis_date})",
                file=sys.stderr,
            )
            sys.exit(1)

        # Check if comparison file exists for this margin
        comparison_file_exists = check_comparison_file_exists_for_margin(
            task_name, analysis_date, args.margin
        )

        if not comparison_file_exists:
            print(
                f"Comparison file for margin {args.margin}% does not exist. Creating new"
                f" comparison files...",
                file=sys.stderr,
            )

            # Need to generate recommendations for all base metrics with this margin
            # Load CSV data from analyzed_data
            csv_data = analyzed_data.get("csv_data")
            if not csv_data:
                print("Error: CSV data not found in analyzed data", file=sys.stderr)
                sys.exit(1)

            # Parse CSV and regenerate recommendations for all base metrics
            data = parse_csv_data(csv_data)
            if not data:
                print("Error: Could not parse CSV data", file=sys.stderr)
                sys.exit(1)

            # Group by step
            by_step = defaultdict(list)
            for row in data:
                step = row.get("step", "").strip()
                if step:
                    by_step[step].append(row)

            # Analyze each step for all base metrics
            all_recommendations_by_base = {"max": [], "p95": [], "p90": [], "median": []}
            for step_name in sorted(by_step.keys()):
                step_all_bases = analyze_step_data_all_bases(
                    step_name, by_step[step_name], args.margin
                )
                if step_all_bases:
                    for base in ["max", "p95", "p90", "median"]:
                        all_recommendations_by_base[base].append(step_all_bases[base])

            steps_missing_obs = analyzed_data.get("steps_without_observability_data")
            if steps_missing_obs is None:
                steps_missing_obs = compute_steps_missing_observability(file_steps, by_step)
            # Save comparison data with new margin (no timestamp since no re-analysis)
            html_path, json_path = save_comparison_data_all_bases(
                task_name,
                all_recommendations_by_base,
                current_resources,
                args.margin,
                analysis_date,
                use_timestamp=False,
                steps_without_observability_data=steps_missing_obs,
            )
            print(f"Created comparison files for margin {args.margin}%:", file=sys.stderr)
            print(f"  - {html_path}", file=sys.stderr)
            print(f"  - {json_path}", file=sys.stderr)
        else:
            print(
                f"Comparison file for margin {args.margin}% already exists. Using existing file.",
                file=sys.stderr,
            )
            # Load existing comparison data
            comparison_data = load_comparison_data(task_name, analysis_date, args.margin)
            if not comparison_data:
                print("Error: Could not load comparison data", file=sys.stderr)
                sys.exit(1)
            all_recommendations_by_base = comparison_data.get("recommendations_by_base", {})

        # Show comparison tables for all base metrics (--base is ignored in Phase 2)
        print(
            f"\nShowing recommendations for all base metrics (margin: {args.margin}%):",
            file=sys.stderr,
        )
        print("Note: --base flag is ignored. All base metrics are shown.", file=sys.stderr)
        print()

        # Show comparison table for each base metric
        for base in ["max", "p95", "p90", "median"]:
            recommendations = all_recommendations_by_base.get(base, [])
            if recommendations:
                print(f"\n{'=' * 100}", file=sys.stderr)
                print(f"Base Metric: {base.upper()}", file=sys.stderr)
                print(f"{'=' * 100}", file=sys.stderr)
                print_comparison_table(
                    recommendations, current_resources, task_name, save_html=False
                )

        print(
            "\nNote: YAML file is NOT updated automatically. Please review the recommendations"
            "above",
            file=sys.stderr,
        )
        print("      and update the YAML file manually if needed.", file=sys.stderr)

        return

    # Phase 1: ANALYSIS (default, unless --update is specified)
    # --base is ignored in Phase 1, all base metrics are generated
    # Determine input source
    current_resources = None
    file_path_or_url = args.file

    if args.file:
        # Load YAML and extract task info
        yaml_content, yaml_path = fetch_yaml_content(args.file)
        yaml_task_name, yaml_steps, default_resources, current_resources = extract_task_info(
            yaml_content
        )

        if not yaml_task_name:
            print("Error: Could not extract task name from YAML", file=sys.stderr)
            sys.exit(1)

        if not yaml_steps:
            print("Error: Could not extract steps from YAML", file=sys.stderr)
            sys.exit(1)

        # Read wrapper script configuration
        script_dir = Path(__file__).parent
        wrapper_path = script_dir / "wrapper_for_promql_for_all_clusters.sh"
        wrapper_task, wrapper_steps, wrapper_defined = read_wrapper_config(wrapper_path)

        # Determine which task/steps to use
        final_task_name = yaml_task_name
        final_steps = yaml_steps
        source_desc = "extracted from YAML file"

        if wrapper_defined:
            # Validate wrapper-defined values against YAML
            print("\n" + "=" * 80, file=sys.stderr)
            print("VALIDATION: Wrapper-defined Task and Steps", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            print(f"Wrapper Task: {wrapper_task}", file=sys.stderr)
            print(f"Wrapper Steps: {', '.join(wrapper_steps)}", file=sys.stderr)
            print(f"YAML Task: {yaml_task_name}", file=sys.stderr)
            print(f"YAML Steps: {', '.join(yaml_steps)}", file=sys.stderr)
            print("=" * 80, file=sys.stderr)

            is_valid, errors = validate_wrapper_steps(
                wrapper_task, wrapper_steps, yaml_task_name, yaml_steps
            )

            if not is_valid:
                print("\nERROR: Validation failed:", file=sys.stderr)
                for error in errors:
                    print(f"  - {error}", file=sys.stderr)
                print(
                    "\nPlease fix the wrapper script or use a different YAML file.", file=sys.stderr
                )
                sys.exit(1)

            # Show which steps will be used vs available
            wrapper_steps_set = set(wrapper_steps)
            yaml_steps_set = set(yaml_steps)
            missing_steps = yaml_steps_set - wrapper_steps_set

            if missing_steps:
                print(
                    f"\nINFO: YAML file has additional steps not in wrapper:"
                    f" {sorted(missing_steps)}",
                    file=sys.stderr,
                )
                print("  These steps will not be analyzed.", file=sys.stderr)

            print(
                "\n✓ Validation passed: Wrapper steps are valid subset of YAML steps",
                file=sys.stderr,
            )

            # Use wrapper's values
            final_task_name = wrapper_task
            final_steps = wrapper_steps
            source_desc = "defined in wrapper script (validated against YAML)"
        else:
            # Extract from YAML and prefix steps with 'step-'
            final_steps = [f"step-{s}" if not s.startswith("step-") else s for s in yaml_steps]
            source_desc = "extracted from YAML file"

        # Always check cluster connectivity (before proceeding with analysis)
        print("\n" + "=" * 80, file=sys.stderr)
        if args.dry_run:
            print("DRY-RUN: Checking Cluster Connectivity", file=sys.stderr)
        else:
            print("Checking Cluster Connectivity", file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        all_connected, connectivity_report = check_cluster_connectivity(wrapper_path)

        print("\nCluster Connectivity Report:", file=sys.stderr)
        for cluster, connected, message in connectivity_report:
            status = "✓" if connected else "✗"
            print(f"  {status} {cluster}: {message}", file=sys.stderr)

        accessible_count = sum(1 for _, connected, _ in connectivity_report if connected)
        total_count = len(connectivity_report)

        cluster_summary = f"{accessible_count}/{total_count} clusters are accessible"
        if not all_connected:
            print(f"\nWARNING: {cluster_summary}.", file=sys.stderr)
            print(
                "  Data collection may fail for unreachable clusters.",
                file=sys.stderr,
            )
            if not args.dry_run:
                print(
                    "  Continuing with accessible clusters only...",
                    file=sys.stderr,
                )
        else:
            print(f"\n✓ {cluster_summary}", file=sys.stderr)

        # Always prompt for confirmation
        # For display, ensure steps have 'step-' prefix (for consistency with wrapper script format)
        steps_for_display = [f"step-{s}" if not s.startswith("step-") else s for s in final_steps]
        if not prompt_confirmation(final_task_name, steps_for_display, source_desc):
            print("Aborted by user.", file=sys.stderr)
            sys.exit(0)

        # If dry-run, exit here
        if args.dry_run:
            print("\n" + "=" * 80, file=sys.stderr)
            print("DRY-RUN completed successfully. No data collection performed.", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            sys.exit(0)

        # Convert final_steps to list without leading 'step-' prefix for collection (step names in
        # reports stay
        # without prefix). Use prefix-only strip — do not use str.replace('step-',''), names like
        # step-fips-operator-check-step-action contain 'step-' inside the suffix and would be
        # corrupted.
        steps_for_collection = [normalize_step_name_for_compare(s) for s in final_steps]

        # Single source of truth: collect detailed per-pod data, then derive CSV for analysis
        parallel_workers = args.pll_clusters if args.pll_clusters and not args.update else None
        pll_queries = max(1, min(4, args.pll_queries)) if args.pll_queries else 2
        detailed_executions, collection_stats = collect_individual_pod_executions(
            final_task_name,
            steps_for_collection,
            days=args.days,
            hours=args.hours,
            lookback_seconds=lookback_seconds,
            parallel_clusters=parallel_workers,
            debug=args.debug,
            current_resources=current_resources,
            pll_queries=pll_queries,
        )

        # Finding 2: compute and print cluster data coverage report
        cluster_coverage_report = compute_cluster_coverage_report(
            detailed_executions, lookback_days_fraction
        )
        if cluster_coverage_report:
            print("\n" + "=" * 80, file=sys.stderr)
            print("Cluster Data Coverage Report (oldest data point per cluster):", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            any_short = False
            for cl in sorted(cluster_coverage_report):
                info = cluster_coverage_report[cl]
                ok = info["meets_requested"]
                mark = "✓" if ok else "⚠"
                print(
                    f"  {mark} {cl}: {info['days_covered']:.1f} days covered "
                    f"(oldest: {info['oldest_date']}, pods: {info['pod_count']})",
                    file=sys.stderr,
                )
                if not ok:
                    any_short = True
            if any_short:
                print(
                    f"\n  WARNING: Some clusters returned fewer than {lookback_label} of data.",
                    file=sys.stderr,
                )
                print(
                    "  Statistics may under-represent rare heavy workloads on those clusters.",
                    file=sys.stderr,
                )
            print("=" * 80, file=sys.stderr)

        csv_data = detailed_executions_to_csv(detailed_executions)
        if not csv_data or not csv_data.strip() or csv_data.count("\n") < 1:
            listed = collection_stats.get("pods_listed", 0)
            queried = collection_stats.get("pods_queried", 0)
            kept = collection_stats.get("pods_kept", 0)
            qfail = collection_stats.get("query_failures", 0)
            empty = collection_stats.get("empty_metrics", 0)
            parse_err = collection_stats.get("parse_errors", 0)
            list_fail = collection_stats.get("list_failures", 0)
            if listed == 0:
                print(
                    "Error: No data from detailed collection — 0 pods listed across all clusters.",
                    file=sys.stderr,
                )
            else:
                print(
                    "Error: No data from detailed collection — "
                    f"{listed} pods listed, {queried} pod×step queries, "
                    f"{kept} kept "
                    f"(query_failures={qfail}, empty_metrics={empty}, "
                    f"parse_errors={parse_err}, list_failures={list_fail}).",
                    file=sys.stderr,
                )
            if steps_for_collection:
                print(f"\nTask: {final_task_name}", file=sys.stderr)
                print(f"Steps: {', '.join(steps_for_collection)}", file=sys.stderr)
                print(f"Lookback: {lookback_label}", file=sys.stderr)
            if args.debug and collection_stats.get("debug_samples"):
                print(
                    "\nRe-run tip: inspect DEBUG skip samples above for root cause.",
                    file=sys.stderr,
                )
            sys.exit(1)
        # Show table (same as wrapper path)
        format_script = Path(__file__).parent / "format_csv_table.py"
        if format_script.exists():
            try:
                result = subprocess.run(
                    [sys.executable, str(format_script)],
                    input=csv_data,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode == 0 and result.stdout:
                    print(result.stdout)
            except (subprocess.TimeoutExpired, Exception):
                pass
        task_name = final_task_name
        steps = steps_for_collection
    else:
        # Read from stdin (e.g. user ran wrapper and piped CSV)
        detailed_executions = None
        cluster_coverage_report = {}
        if sys.stdin.isatty():
            print("Error: No input provided. Use --file or pipe CSV data.", file=sys.stderr)
            sys.exit(1)
        csv_data = sys.stdin.read()
        yaml_content = None
        task_name = None
        steps = None
        file_path_or_url = None

    # Parse CSV data
    data = parse_csv_data(csv_data)
    if not data:
        print("Error: No data found in CSV input", file=sys.stderr)
        print("\nPossible reasons:", file=sys.stderr)
        print(
            "  1. No pods found for the specified task/steps in the given time period",
            file=sys.stderr,
        )
        print(
            "  2. Task or step names don't match what's actually running in clusters",
            file=sys.stderr,
        )
        print("  3. Cluster connectivity issues (try --dry-run to check)", file=sys.stderr)
        print(
            "  4. Time period too short (try increasing --days / --hours)",
            file=sys.stderr,
        )
        if args.file:
            print(f"\nTask: {task_name}", file=sys.stderr)
            print(f"Steps: {', '.join(steps)}", file=sys.stderr)
            print(f"Lookback: {lookback_label}", file=sys.stderr)
        # Show first few lines of CSV to help debug
        if csv_data:
            csv_lines = csv_data.strip().split("\n")
            print(f"\nCSV output (first {min(5, len(csv_lines))} lines):", file=sys.stderr)
            for i, line in enumerate(csv_lines[:5], 1):
                print(f"  {i}: {line[:100]}", file=sys.stderr)
        sys.exit(1)

    # Group by step
    by_step = defaultdict(list)
    for row in data:
        step = row.get("step", "").strip()
        if step:
            by_step[step].append(row)

    steps_missing_obs = []
    if steps:
        steps_missing_obs = compute_steps_missing_observability(steps, by_step)
        if steps_missing_obs:
            print("", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            print("WARNING: YAML step(s) with no observability data in this run", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            for s in steps_missing_obs:
                print(f"  - {s}", file=sys.stderr)
            print("", file=sys.stderr)
            print(
                "Recommendations and aggregate tables only include steps with at least one",
                file=sys.stderr,
            )
            print(
                'Prometheus sample for container="step-<name>" in the analysis window.',
                file=sys.stderr,
            )
            print(
                "Common causes: TaskRuns using older bundles without that step; git-resolved",
                file=sys.stderr,
            )
            print(
                "StepAction with a different runtime container name; or no pods in --days.",
                file=sys.stderr,
            )
            print("=" * 80, file=sys.stderr)

    # Phase 1: Analyze each step for ALL base metrics (ignore --base flag)
    # Note: --base is ignored in Phase 1, all metrics (max, p95, p90, median) are generated
    all_recommendations_by_base = {"max": [], "p95": [], "p90": [], "median": []}
    for step_name in sorted(by_step.keys()):
        step_all_bases = analyze_step_data_all_bases(step_name, by_step[step_name], args.margin)
        if step_all_bases:
            for base in ["max", "p95", "p90", "median"]:
                all_recommendations_by_base[base].append(step_all_bases[base])

    # Get date string for file naming (YYYYMMDD format)
    date_str = datetime.now().strftime("%Y%m%d")

    # Check if analyzed_data files exist for this date (indicates re-analysis)
    # This check happens BEFORE saving, so we know if we need timestamp
    analyzed_files_exist = (
        check_files_exist_for_date(task_name, "analyzed_data", date_str)
        if file_path_or_url and task_name
        else False
    )

    # Save analyzed_data (CSV data) - use timestamp if re-analysis happened
    analyzed_html_path = None
    analyzed_json_path = None
    if file_path_or_url and task_name and csv_data:
        analyzed_html_path, analyzed_json_path = save_analyzed_data(
            task_name,
            csv_data,
            date_str,
            steps_without_observability_data=steps_missing_obs,
            cluster_coverage_report=cluster_coverage_report if args.file else None,
            days_requested=lookback_days_fraction if args.file else None,
        )
        print("\nSaved analyzed data:", file=sys.stderr)
        print(f"  - {analyzed_html_path}", file=sys.stderr)
        print(f"  - {analyzed_json_path}", file=sys.stderr)

    # Save comparison_data (all base metrics) - use timestamp if re-analysis happened
    comparison_html_path = None
    comparison_json_path = None
    if file_path_or_url and task_name:
        comparison_html_path, comparison_json_path = save_comparison_data_all_bases(
            task_name,
            all_recommendations_by_base,
            current_resources,
            args.margin,
            date_str,
            use_timestamp=analyzed_files_exist,
            steps_without_observability_data=steps_missing_obs,
            cluster_coverage_report=cluster_coverage_report if args.file else None,
            days_requested=lookback_days_fraction if args.file else None,
            detailed_executions=detailed_executions if detailed_executions else None,
        )
        print(
            f"\nSaved comparison data (all base metrics, margin={args.margin}%):", file=sys.stderr
        )
        print(f"  - {comparison_html_path}", file=sys.stderr)
        print(f"  - {comparison_json_path}", file=sys.stderr)

    # Save detailed per-step data (one HTML/JSON/CSV per step); use already-collected executions
    # when from --file (single source)
    if file_path_or_url and task_name and steps and detailed_executions:
        detailed_paths = save_detailed_per_step_data(task_name, detailed_executions, date_str)
        print("\nSaved detailed per-step data (one file per step):", file=sys.stderr)
        for html_path, json_path, csv_path in detailed_paths:
            print(f"  - {html_path}", file=sys.stderr)
            print(f"  - {json_path}", file=sys.stderr)
            print(f"  - {csv_path}", file=sys.stderr)

    # In-memory verification: aggregated main table vs recomputation from detailed executions (only
    # when we have both)
    if detailed_executions and data:
        verify_ok, verify_messages = verify_aggregates_against_detailed(detailed_executions, data)
        if verify_messages:
            print("\n" + "=" * 80, file=sys.stderr)
            print("Aggregate verification (main table vs detailed data):", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            for msg in verify_messages:
                print(f"  {msg}", file=sys.stderr)
        if verify_ok:
            print(
                "\n✓ Aggregate verification passed: main table matches recomputation from"
                "detailed executions.",
                file=sys.stderr,
            )
        else:
            print(
                "\n✗ Aggregate verification found mismatches (see above). Please report if this"
                "persists.",
                file=sys.stderr,
            )

    print(
        "\nPhase 1 (Analysis) completed. All base metrics (max, p95, p90, median) have been"
        "generated.",
        file=sys.stderr,
    )
    if steps_missing_obs:
        print(
            f"Note: {len(steps_missing_obs)} YAML step(s) had no observability data (see"
            f" WARNING above; "
            f"also steps_without_observability_data in saved JSON).",
            file=sys.stderr,
        )
    if comparison_html_path:
        print(
            "\nReview the comparison report to choose your base metric (max / p95 / p90 / median):",
            file=sys.stderr,
        )
        print(f"  → {comparison_html_path}", file=sys.stderr)
    print(
        "\nRun Phase 2 (--update) with --file to apply recommendations for a specific base metric.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
