#!/usr/bin/env python3
"""
oc_get_ooms.py

Detect OOMKilled and CrashLoopBackOff pods across multiple OpenShift/Kubernetes contexts,
parallelized at cluster and namespace levels, with artifact collection.

New in this version:
- When a pod is detected as OOMKilled or CrashLoopBackOff, save:
    - `oc describe pod <pod>` output
    - One log file with `oc logs <pod> --previous` (crashed container)
      then `oc logs <pod>` (current), appended
  into per-cluster directories under output/logs_and_description_files/<cluster>/
  Filenames include namespace, pod name, and timestamp to avoid collisions.
- CSV and JSON now include the absolute paths to the description and pod log files:
    description_file, pod_log_file

All previously requested features retained:
- cluster parallelism, namespace batching, include/exclude regex, retries, timeouts,
  time range filtering, colorized output, etc.
"""

from __future__ import annotations

import atexit
import contextlib
import csv
import glob
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from re import Pattern
from typing import Any

# Import HTML export module
try:
    from html_export import generate_html_report
except ImportError:
    # Fallback if module not found
    generate_html_report = None


# ---------------------------
# Color output
# ---------------------------
RED = "\033[1;31m"
GREEN = "\033[1;32m"
YELLOW = "\033[1;33m"
BLUE = "\033[1;34m"
RESET = "\033[0m"


def color(text: str, c: str) -> str:
    return f"{c}{text}{RESET}"


# ---------------------------
# Time windows (kept for context; not used directly for artifact collection)
# ---------------------------
TIME_WINDOWS = {
    "last_1h": 1,
    "last_3h": 3,
    "last_6h": 6,
    "last_24h": 24,
    "last_48h": 48,
    "last_3d": 72,
    "last_5d": 120,
    "last_7d": 168,
}


# ---------------------------
# Defaults
# ---------------------------
DEFAULT_RETRIES = 3
DEFAULT_OC_TIMEOUT = 45  # seconds
RETRY_DELAY_SECONDS = 3

# konflux-release-data (CODEOWNERS for namespace owners);
# same default as oom_logs_and_desc_bundle_generator
KONFLUX_RELEASE_DATA_REPO = "git@gitlab.cee.redhat.com:releng/konflux-release-data.git"
_CODEOWNERS_TEMP_DIR: str | None = None
_CODEOWNERS_ATEXIT_REGISTERED = False
DEFAULT_NS_BATCH_SIZE = 10
DEFAULT_NS_WORKERS = 5
DEFAULT_BATCH_SIZE = 2


# ---------------------------
# Command runner with retries
# ---------------------------
def run_cmd_with_retries(
    cmd: list[str], retries: int = DEFAULT_RETRIES, timeout: int | None = None
) -> tuple[int, str, str]:
    attempt = 0
    last_err = ""
    while attempt < max(1, retries):
        attempt += 1
        try:
            completed = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            stdout = (completed.stdout or "").strip()
            stderr = (completed.stderr or "").strip()
            return completed.returncode, stdout, stderr
        except subprocess.TimeoutExpired:
            last_err = f"TimeoutExpired after {timeout}s"
            time.sleep(RETRY_DELAY_SECONDS * attempt)
        except Exception as e:
            last_err = str(e)
            time.sleep(RETRY_DELAY_SECONDS * attempt)
    return 1, "", last_err


def run_shell_cmd_with_retries(
    cmd: str, retries: int = DEFAULT_RETRIES, timeout: int | None = None
) -> tuple[int, str, str]:
    return run_cmd_with_retries(["/bin/sh", "-c", cmd], retries=retries, timeout=timeout)


# ---------------------------
# CLI tool detection and helpers
# ---------------------------
_CLI_TOOL: str | None = None  # Cached CLI tool (kubectl or oc)


def detect_cli_tool() -> str:
    """
    Detect which CLI tool to use: kubectl (preferred) or oc (fallback).

    Returns:
        "kubectl" if available, "oc" if kubectl not available, or raises error if neither found
    """
    global _CLI_TOOL
    if _CLI_TOOL:
        return _CLI_TOOL

    # Try kubectl first (works with any Kubernetes cluster)
    rc, _, _ = run_cmd_with_retries(
        ["kubectl", "version", "--client", "--short"], retries=1, timeout=5
    )
    if rc == 0:
        _CLI_TOOL = "kubectl"
        return _CLI_TOOL

    # Fallback to oc (OpenShift)
    rc, _, _ = run_cmd_with_retries(["oc", "version", "--client"], retries=1, timeout=5)
    if rc == 0:
        _CLI_TOOL = "oc"
        return _CLI_TOOL

    # Neither found
    raise RuntimeError(
        "Neither 'kubectl' nor 'oc' CLI tool found. "
        "Please install kubectl (for Kubernetes) or oc (for OpenShift)."
    )


def cli_cmd_parts(context: str, cli_timeout_seconds: int, subcommand: list[str]) -> list[str]:
    """Build command parts for kubectl or oc."""
    cli_tool = detect_cli_tool()
    parts = [cli_tool, f"--request-timeout={cli_timeout_seconds}s"]
    if context:
        parts += ["--context", context]
    parts += subcommand
    return parts


def run_cli_subcommand(
    context: str, subcommand: list[str], retries: int, cli_timeout_seconds: int
) -> tuple[int, str, str]:
    """Run a kubectl or oc subcommand."""
    cmd = cli_cmd_parts(context, cli_timeout_seconds, subcommand)
    return run_cmd_with_retries(cmd, retries=retries, timeout=cli_timeout_seconds + 5)


# Backward compatibility aliases
def oc_cmd_parts(context: str, oc_timeout_seconds: int, subcommand: list[str]) -> list[str]:
    """Backward compatibility alias for cli_cmd_parts."""
    return cli_cmd_parts(context, oc_timeout_seconds, subcommand)


def run_oc_subcommand(
    context: str, subcommand: list[str], retries: int, oc_timeout_seconds: int
) -> tuple[int, str, str]:
    """Backward compatibility alias for run_cli_subcommand."""
    return run_cli_subcommand(context, subcommand, retries, oc_timeout_seconds)


# ---------------------------
# context utilities
# ---------------------------
def get_all_contexts(retries: int, oc_timeout_seconds: int) -> list[str]:
    """Get all available Kubernetes/OpenShift contexts."""
    cli_tool = detect_cli_tool()
    cmd = [cli_tool, "config", "get-contexts", "-o", "name"]
    rc, out, err = run_cmd_with_retries(cmd, retries=retries, timeout=oc_timeout_seconds + 5)
    if rc != 0 or not out:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


def match_contexts_by_substring(
    substrings: list[str],
    available_contexts: list[str],
) -> list[str]:
    """
    Match context substrings against available contexts.

    Args:
        substrings: List of substrings to match (e.g., ['kflux-prd-rh02'])
        available_contexts: List of all available context names

    Returns:
        List of matched full context names

    Raises:
        SystemExit: If no match or multiple matches found for a substring
    """
    matched_contexts = []
    for substring in substrings:
        matches = [ctx for ctx in available_contexts if substring.lower() in ctx.lower()]
        if not matches:
            print(
                color(
                    f"ERROR: No context found matching substring '{substring}'",
                    RED,
                )
            )
            print(color("Available contexts:", YELLOW))
            for ctx in available_contexts:
                print(f"  - {ctx}")
            sys.exit(1)
        elif len(matches) > 1:
            print(
                color(
                    f"ERROR: Multiple contexts match substring '{substring}':",
                    RED,
                )
            )
            for ctx in matches:
                print(f"  - {ctx}")
            print(
                color(
                    "Please use a more specific substring to uniquely identify the context.",
                    YELLOW,
                )
            )
            sys.exit(1)
        else:
            matched_contexts.append(matches[0])
            print(
                color(
                    f"Matched '{substring}' -> '{matches[0]}'",
                    GREEN,
                )
            )
    return matched_contexts


def get_current_context(retries: int, oc_timeout_seconds: int) -> str:
    """Get the current Kubernetes/OpenShift context."""
    cli_tool = detect_cli_tool()
    cmd = [cli_tool, "config", "current-context"]
    rc, out, err = run_cmd_with_retries(cmd, retries=retries, timeout=oc_timeout_seconds + 5)
    return out.strip() if rc == 0 else ""


def short_cluster_name(full_ctx: str) -> str:
    m = re.search(r"api-([^-]+-[^-]+-[^-]+)", full_ctx)
    if m:
        return m.group(1)
    if "/" in full_ctx:
        return full_ctx.split("/")[-1]
    return full_ctx.replace("/", "_").replace(":", "_")


# ---------------------------
# timestamp utilities
# ---------------------------
def parse_timestamp_to_iso(ts: str) -> str:
    """Parse Kubernetes timestamp to ISO format."""
    if not ts:
        return ""
    try:
        base = ts.split(".")[0].rstrip("Z")
        dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, AttributeError) as e:
        logging.debug(f"Failed to parse timestamp '{ts}': {e}")
        return ts


def _parse_kubernetes_timestamp_utc(ts: str) -> float | None:
    """
    Parse a Kubernetes timestamp string (RFC3339, typically UTC with Z) to Unix seconds.
    Returns None if ts is empty or unparseable.
    """
    if not ts or not ts.strip():
        return None
    try:
        base = ts.split(".")[0].rstrip("Z")
        dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=UTC).timestamp()
    except (ValueError, AttributeError):
        return None


def _timestamp_in_range(ts_str: str, cutoff_time: float) -> bool:
    """
    Return True if the finding should be included for time-range filtering.
    - If ts_str is empty: include (we don't drop findings with no timestamp).
    - Otherwise: include only if parsed timestamp (as UTC) >= cutoff_time.
    """
    if not ts_str or not ts_str.strip():
        return True
    parsed = _parse_kubernetes_timestamp_utc(ts_str)
    if parsed is None:
        return True
    return parsed >= cutoff_time


def now_ts_for_filename() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def timestamp_for_backup() -> str:
    """Generate a readable timestamp string for backup filenames.

    Returns format like: '12-Jan-2026_12-05-57-EST'
    """
    now = datetime.now()
    # Get timezone abbreviation (EST, PST, etc.)
    tz_abbr = "UTC"
    try:
        # Try strftime first
        tz_str = now.strftime("%Z")
        if tz_str and tz_str.strip():
            tz_abbr = tz_str
        else:
            # Fallback: use time.tzname
            import time

            if time.tzname and len(time.tzname) > 0:
                tz_abbr = time.tzname[0] if time.daylight == 0 else time.tzname[1]
    except Exception:
        # If all else fails, use UTC
        tz_abbr = "UTC"

    # Format: DD-MMM-YYYY_HH-MM-SS-TZ
    return now.strftime(f"%d-%b-%Y_%H-%M-%S-{tz_abbr}")


def report_generated_est() -> str:
    """Return current time formatted for report header, preferably in EST (America/New_York)."""
    try:
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("America/New_York"))
        return now.strftime("%d-%b-%Y %H:%M:%S %Z")
    except Exception:
        now = datetime.now(UTC)
        return now.strftime("%d-%b-%Y %H:%M:%S UTC")


def timestamp_for_backup_from_file(file_path: Path) -> str:
    """Generate a timestamp string for backup filenames using the file's last modified time.

    Same format as timestamp_for_backup(): e.g. '02-Feb-2026_10-38-49-EDT'.
    This keeps backup names aligned with the file's actual modification date.
    """
    mtime = file_path.stat().st_mtime
    dt = datetime.fromtimestamp(mtime)
    tz_abbr = "UTC"
    try:
        tz_str = dt.strftime("%Z")
        if tz_str and tz_str.strip():
            tz_abbr = tz_str
        else:
            if time.tzname and len(time.tzname) > 0:
                tz_abbr = time.tzname[0] if time.daylight == 0 else time.tzname[1]
    except Exception:
        tz_abbr = "UTC"
    return dt.strftime(f"%d-%b-%Y_%H-%M-%S-{tz_abbr}")


# ---------------------------
# connectivity
# ---------------------------
def check_cluster_connectivity(
    context: str, retries: int, oc_timeout_seconds: int
) -> tuple[bool, str]:
    """Check cluster connectivity using appropriate method for the CLI tool."""
    cli_tool = detect_cli_tool()

    # oc has 'whoami', kubectl doesn't - use 'get ns' for kubectl
    if cli_tool == "oc":
        rc, out, err = run_cli_subcommand(
            context, ["whoami"], retries=retries, cli_timeout_seconds=oc_timeout_seconds
        )
    else:  # kubectl
        # Use 'get ns' as connectivity check (works for all auth methods)
        # Note: --request-timeout is already added by cli_cmd_parts, so we don't need it here
        rc, out, err = run_cli_subcommand(
            context, ["get", "ns"], retries=retries, cli_timeout_seconds=oc_timeout_seconds
        )

    if rc == 0:
        return True, ""
    return False, err or out or "unknown error"


def check_all_clusters_connectivity(
    contexts: list[str], retries: int, oc_timeout_seconds: int
) -> tuple[bool, list[tuple[str, bool, str]]]:
    """
    Check connectivity to all clusters.

    Returns:
        tuple: (all_connected, connectivity_report)
        - all_connected: True if all clusters are accessible
        - connectivity_report: List of (cluster_name, connected, error_message) tuples
    """
    report = []
    all_connected = True

    print(color("\n" + "=" * 80, BLUE))
    print(color("Checking Cluster Connectivity", BLUE))
    print(color("=" * 80, BLUE))

    for ctx in contexts:
        cluster = short_cluster_name(ctx)
        connected, error_msg = check_cluster_connectivity(
            ctx, retries=retries, oc_timeout_seconds=oc_timeout_seconds
        )
        if connected:
            report.append((cluster, True, "Connected"))
            print(color(f"  ✓ {cluster}: Connected", GREEN))
        else:
            report.append((cluster, False, error_msg))
            print(color(f"  ✗ {cluster}: {error_msg}", RED))
            all_connected = False

    print(color("=" * 80, BLUE))

    return all_connected, report


def print_connectivity_report_summary(connectivity_report: list[tuple[str, bool, str]]) -> None:
    """
    Print the Cluster Connectivity Report summary (second block).
    Does not prompt for user input.
    """
    print(color("\nCluster Connectivity Report:", BLUE))
    for cluster, connected, message in connectivity_report:
        if connected:
            print(color(f"  ✓ {cluster}: {message}", GREEN))
        else:
            print(color(f"  ✗ {cluster}: {message}", RED))

    all_connected = all(connected for _, connected, _ in connectivity_report)
    if all_connected:
        print(color("\n✓ All clusters are accessible", GREEN))
    else:
        print(color("\nWARNING: Some clusters are not accessible.", YELLOW))
        print(color("  Data collection may fail for these clusters.", YELLOW))
        print(color("  Continuing with accessible clusters only...", YELLOW))

    print(color("=" * 80, BLUE))


# ---------------------------
# oc namespace workers
# ---------------------------
def parse_time_range(time_range_str: str) -> int:
    """
    Parse time range string (e.g., '1d', '2h', '30m', '1M') into seconds.
    Returns seconds from now to look back.
    """
    if not time_range_str:
        return 86400  # Default 1 day
    time_range_str = time_range_str.strip()
    # Do not lower: m=minutes, M=months (30 days)
    match = re.match(r"^(\d+)([smhdM])$", time_range_str)
    if not match:
        raise ValueError(f"Invalid time range format: {time_range_str}")
    value = int(match.group(1))
    unit = match.group(2)
    multipliers = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "M": 2592000,  # 30 days
    }
    return value * multipliers.get(unit, 86400)


def get_all_events_oc(
    context: str,
    namespace: str,
    retries: int,
    oc_timeout_seconds: int,
    time_range_seconds: int | None = None,
) -> list[dict[str, Any]]:
    """
    Get all events for a namespace (single API call for efficiency).

    Args:
        time_range_seconds: If provided, filter events to this time range
    """
    subcmd = ["-n", namespace, "get", "events", "--ignore-not-found", "-o", "json"]
    rc, out, err = run_oc_subcommand(
        context, subcmd, retries=retries, oc_timeout_seconds=oc_timeout_seconds
    )
    if rc != 0 or not out:
        return []
    try:
        obj = json.loads(out)
    except json.JSONDecodeError as e:
        logging.warning(f"Failed to parse events JSON for {namespace}: {e}")
        return []
    events = obj.get("items", [])

    # Filter by time range if provided (Kubernetes event timestamps are UTC)
    if time_range_seconds:
        cutoff_time = datetime.now(UTC).timestamp() - time_range_seconds
        filtered_events = []
        for ev in events:
            ts = ev.get("eventTime") or ev.get("lastTimestamp") or ev.get("firstTimestamp")
            if ts:
                try:
                    # Parse as UTC and compare with cutoff
                    ev_ts = _parse_kubernetes_timestamp_utc(ts)
                    if ev_ts is not None and ev_ts >= cutoff_time:
                        filtered_events.append(ev)
                    elif ev_ts is None:
                        # Unparseable, include to be safe
                        filtered_events.append(ev)
                except (ValueError, AttributeError):
                    filtered_events.append(ev)
            else:
                filtered_events.append(ev)
        return filtered_events

    return events


def _application_component_from_labels(labels: dict[str, str] | None) -> tuple[str, str]:
    """Extract Application and Component from pod metadata.labels.

    Application: appstudio.openshift.io/application (Konflux), then standard Kubernetes labels.
    Component: tekton.dev/pipelineTask (Tekton step), tekton.dev/task, then standard labels.
    """
    if not labels:
        return "", ""
    application = (
        labels.get("appstudio.openshift.io/application")
        or labels.get("app.kubernetes.io/part-of")
        or labels.get("app.kubernetes.io/name")
        or labels.get("app")
        or ""
    ).strip()
    component = (
        labels.get("tekton.dev/pipelineTask")
        or labels.get("tekton.dev/task")
        or labels.get("app.kubernetes.io/component")
        or labels.get("component")
        or ""
    ).strip()
    return application, component


def get_pods_items(
    context: str,
    namespace: str,
    retries: int,
    oc_timeout_seconds: int,
) -> list[dict[str, Any]]:
    """Fetch pods in namespace as list of pod items.

    For reuse in OOM/Crash detection and labels.
    """
    subcmd = ["-n", namespace, "get", "pods", "-o", "json", "--ignore-not-found"]
    rc, out, err = run_oc_subcommand(
        context, subcmd, retries=retries, oc_timeout_seconds=oc_timeout_seconds
    )
    if rc != 0 or not out:
        return []
    try:
        obj = json.loads(out)
    except json.JSONDecodeError as e:
        logging.warning(f"Failed to parse pods JSON for {namespace}: {e}")
        return []
    return obj.get("items", [])


def find_events_by_reason_oc(
    context: str,
    namespace: str,
    reason_substring: str,
    retries: int,
    oc_timeout_seconds: int,
    time_range_seconds: int | None = None,
) -> list[dict[str, str]]:
    """Find events matching a reason substring in a namespace."""
    events = get_all_events_oc(context, namespace, retries, oc_timeout_seconds, time_range_seconds)
    res: list[dict[str, str]] = []
    for ev in events:
        reason = ev.get("reason", "")
        if reason_substring.lower() not in reason.lower():
            continue
        pod = ev.get("involvedObject", {}).get("name")
        ts = ev.get("eventTime") or ev.get("lastTimestamp") or ev.get("firstTimestamp")
        if pod and ts:
            res.append({"pod": pod, "reason": reason, "timestamp": parse_timestamp_to_iso(ts)})
    return res


def oomkilled_via_pods_oc(
    context: str,
    namespace: str,
    retries: int,
    oc_timeout_seconds: int,
    time_range_seconds: int | None = None,
    items: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    """Find pods that were OOMKilled by querying pod status.

    Enhanced detection checks multiple states:
    - lastState.terminated.reason == "OOMKilled" (previous OOM kill)
    - state.terminated.reason == "OOMKilled" (current/just OOM killed)
    - Also checks initContainerStatuses for init container OOM kills

    When time_range_seconds is set, only include findings whose finishedAt
    is within the window (or include when finishedAt is missing).
    If items is provided (from get_pods_items), uses it and adds application/component from labels.
    """
    if items is None:
        items = get_pods_items(context, namespace, retries, oc_timeout_seconds)
    res: list[dict[str, str]] = []
    seen_pods: set[str] = set()  # Avoid duplicates with timestamps
    cutoff_time: float | None = None
    if time_range_seconds is not None:
        cutoff_time = datetime.now(UTC).timestamp() - time_range_seconds

    for item in items:
        pod_name = item.get("metadata", {}).get("name")
        if not pod_name:
            continue
        app, comp = _application_component_from_labels(item.get("metadata", {}).get("labels"))

        # Check both regular containers and init containers
        container_statuses = item.get("status", {}).get("containerStatuses", []) or []
        init_container_statuses = item.get("status", {}).get("initContainerStatuses", []) or []
        all_statuses = container_statuses + init_container_statuses

        for cs in all_statuses:
            # Check current state.terminated (just OOM killed)
            terminated = cs.get("state", {}).get("terminated", {})
            if terminated and terminated.get("reason") == "OOMKilled":
                finished_at = terminated.get("finishedAt", "")
                if cutoff_time is not None and not _timestamp_in_range(finished_at, cutoff_time):
                    continue
                key = f"{pod_name}:current"
                if key not in seen_pods:
                    res.append(
                        {
                            "pod": pod_name,
                            "reason": "OOMKilled",
                            "timestamp": (
                                parse_timestamp_to_iso(finished_at) if finished_at else ""
                            ),
                            "application": app,
                            "component": comp,
                        }
                    )
                    seen_pods.add(key)
                continue

            # Check lastState.terminated.reason for OOMKilled (previous OOM kill)
            last_state = cs.get("lastState", {})
            last_terminated = last_state.get("terminated", {})
            if last_terminated and last_terminated.get("reason") == "OOMKilled":
                finished_at = last_terminated.get("finishedAt", "")
                if cutoff_time is not None and not _timestamp_in_range(finished_at, cutoff_time):
                    continue
                key = f"{pod_name}:last:{finished_at}"
                if key not in seen_pods:
                    res.append(
                        {
                            "pod": pod_name,
                            "reason": "OOMKilled",
                            "timestamp": (
                                parse_timestamp_to_iso(finished_at) if finished_at else ""
                            ),
                            "application": app,
                            "component": comp,
                        }
                    )
                    seen_pods.add(key)

    return res


def crashloop_via_pods_oc(
    context: str,
    namespace: str,
    retries: int,
    oc_timeout_seconds: int,
    time_range_seconds: int | None = None,
    items: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    """Find pods in CrashLoopBackOff state by querying pod status.

    Enhanced detection checks multiple states:
    - state.waiting.reason == "CrashLoopBackOff" (current waiting state)
    - state.terminated.reason == "CrashLoopBackOff" (just crashed)
    - lastState.terminated.reason == "CrashLoopBackOff" (previous crash)
    - High restart count (restartCount > 0) as indicator of crash loops
    - Also checks initContainerStatuses for init container failures

    When time_range_seconds is set, only include findings that fall within the
    window. If we have a finishedAt timestamp, filter by it; if no timestamp,
    include the finding (don't drop due to missing metadata).
    If items is provided (from get_pods_items), uses it and adds application/component from labels.
    """
    if items is None:
        items = get_pods_items(context, namespace, retries, oc_timeout_seconds)
    res: list[dict[str, str]] = []
    seen_pods: set[str] = set()  # Avoid duplicates
    cutoff_time: float | None = None
    if time_range_seconds is not None:
        cutoff_time = datetime.now(UTC).timestamp() - time_range_seconds

    for item in items:
        pod_name = item.get("metadata", {}).get("name")
        if not pod_name:
            continue
        app, comp = _application_component_from_labels(item.get("metadata", {}).get("labels"))

        # Check both regular containers and init containers
        container_statuses = item.get("status", {}).get("containerStatuses", []) or []
        init_container_statuses = item.get("status", {}).get("initContainerStatuses", []) or []
        all_statuses = container_statuses + init_container_statuses

        for cs in all_statuses:
            # Check current state.waiting (no finishedAt; include if no time filter or by policy)
            waiting = cs.get("state", {}).get("waiting")
            if waiting and waiting.get("reason") == "CrashLoopBackOff":
                # No timestamp for waiting state; include when no time range or always include
                if pod_name not in seen_pods:
                    res.append(
                        {
                            "pod": pod_name,
                            "reason": "CrashLoopBackOff",
                            "timestamp": "",
                            "application": app,
                            "component": comp,
                        }
                    )
                    seen_pods.add(pod_name)
                continue

            # Check current state.terminated (container just crashed)
            terminated = cs.get("state", {}).get("terminated")
            if terminated and terminated.get("reason") == "CrashLoopBackOff":
                finished_at = terminated.get("finishedAt", "")
                if cutoff_time is not None and not _timestamp_in_range(finished_at, cutoff_time):
                    continue
                if pod_name not in seen_pods:
                    res.append(
                        {
                            "pod": pod_name,
                            "reason": "CrashLoopBackOff",
                            "timestamp": parse_timestamp_to_iso(finished_at) if finished_at else "",
                            "application": app,
                            "component": comp,
                        }
                    )
                    seen_pods.add(pod_name)
                continue

            # Check lastState.terminated (previous crash)
            last_state = cs.get("lastState", {})
            last_terminated = last_state.get("terminated", {})
            if last_terminated and last_terminated.get("reason") == "CrashLoopBackOff":
                finished_at = last_terminated.get("finishedAt", "")
                if cutoff_time is not None and not _timestamp_in_range(finished_at, cutoff_time):
                    continue
                if pod_name not in seen_pods:
                    res.append(
                        {
                            "pod": pod_name,
                            "reason": "CrashLoopBackOff",
                            "timestamp": parse_timestamp_to_iso(finished_at) if finished_at else "",
                            "application": app,
                            "component": comp,
                        }
                    )
                    seen_pods.add(pod_name)
                continue

            # Check restart count as indicator of crash loops
            # Only flag if restart count is high (>= 3) AND there's evidence of crashes
            restart_count = cs.get("restartCount", 0)
            if restart_count >= 3:
                has_terminated_state = (
                    cs.get("state", {}).get("terminated") is not None
                    or cs.get("lastState", {}).get("terminated") is not None
                )
                if has_terminated_state and pod_name not in seen_pods:
                    # Use finishedAt from either state for time filter if available
                    finished_at = ""
                    term = cs.get("state", {}).get("terminated") or cs.get("lastState", {}).get(
                        "terminated"
                    )
                    if term:
                        finished_at = term.get("finishedAt", "")
                    if cutoff_time is not None and not _timestamp_in_range(
                        finished_at, cutoff_time
                    ):
                        continue
                    res.append(
                        {
                            "pod": pod_name,
                            "reason": "CrashLoopBackOff",
                            "timestamp": parse_timestamp_to_iso(finished_at) if finished_at else "",
                            "application": app,
                            "component": comp,
                        }
                    )
                    seen_pods.add(pod_name)
                    continue

        # Also check pod phase - Failed or Pending might indicate issues
        pod_phase = item.get("status", {}).get("phase", "")
        if pod_phase == "Failed" and pod_name not in seen_pods:
            has_restarts = any(cs.get("restartCount", 0) > 0 for cs in all_statuses)
            if has_restarts:
                # No specific finishedAt for phase Failed; include (no timestamp)
                res.append(
                    {
                        "pod": pod_name,
                        "reason": "CrashLoopBackOff",
                        "timestamp": "",
                        "application": app,
                        "component": comp,
                    }
                )
                seen_pods.add(pod_name)

    return res


# ---------------------------
# Ephemeral namespace detection
# ---------------------------
def is_ephemeral_namespace(
    namespace_name: str, namespace_metadata: dict[str, Any] | None = None
) -> bool:
    """
    Detect if a namespace is an ephemeral test or cluster namespace.

    Detection methods (in order of reliability):
    1. Label-based detection (most reliable):
       - konflux-ci.dev/namespace-type: eaas (EaaS ephemeral namespaces)
       - Other ephemeral namespace labels
    2. Name pattern matching:
       - Ephemeral cluster namespaces: clusters-<uuid> pattern
       - Ephemeral test namespaces: test-*, e2e-*, ephemeral-*, ci-*, pr-*, temp-*

    Args:
        namespace_name: Name of the namespace
        namespace_metadata: Optional namespace metadata dict (from Kubernetes API)
                          If provided, labels will be checked for more reliable detection

    Returns True if the namespace matches ephemeral patterns or labels.
    """
    if not namespace_name:
        return False

    # Method 1: Check labels (most reliable - works even if namespace name is modified)
    if namespace_metadata:
        labels = namespace_metadata.get("labels", {})
        if labels:
            # Primary check: EaaS ephemeral namespace label (most reliable indicator)
            # konflux-ci.dev/namespace-type: eaas
            if labels.get("konflux-ci.dev/namespace-type") == "eaas":
                return True

            # Check for other ephemeral namespace label indicators
            # Look for labels that suggest ephemeral/test namespaces
            ephemeral_label_indicators = {
                "konflux-ci.dev/namespace-type": ["eaas", "ephemeral", "test"],
                "namespace-type": ["eaas", "ephemeral", "test"],
                "ephemeral": ["true", "yes"],
            }

            for label_key, label_value in labels.items():
                label_key_lower = label_key.lower()
                label_value_lower = str(label_value).lower()

                # Check if label key matches known ephemeral indicators
                for indicator_key, indicator_values in ephemeral_label_indicators.items():
                    if indicator_key in label_key_lower and any(
                        val in label_value_lower for val in indicator_values
                    ):
                        return True

    # Method 2: Name pattern matching (fallback if labels not available)
    # Ephemeral cluster namespaces: clusters-<uuid> pattern
    # UUID format: 8-4-4-4-12 hex digits (e.g., clusters-4e52ba17-c17b-4f35-b7e0-0215e63678a0)
    if re.match(
        r"^clusters-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        namespace_name,
        re.IGNORECASE,
    ):
        return True

    # Ephemeral test namespaces: common test/e2e/ephemeral patterns
    ephemeral_test_patterns = [
        r"^test-",
        r"^e2e-",
        r"^ephemeral-",
        r"^ci-",
        r"^pr-",
        r"^temp-",
        r"^tmp-",
        r"-test$",
        r"-e2e$",
        r"-ephemeral$",
    ]

    for pattern in ephemeral_test_patterns:
        if re.search(pattern, namespace_name, re.IGNORECASE):
            return True

    return False


# ---------------------------
# get namespaces and apply include/exclude regex lists
# ---------------------------
def get_namespaces_for_context(
    context: str,
    retries: int,
    oc_timeout_seconds: int,
    include_patterns: list[Pattern] | None = None,
    exclude_patterns: list[Pattern] | None = None,
    exclude_ephemeral: bool = True,
) -> list[str]:
    """
    Get namespaces for a context, optionally filtered by include/exclude patterns.

    Args:
        exclude_ephemeral: If True, automatically exclude ephemeral test and cluster namespaces
                          (default: True for EaaS clusters)
    """
    subcmd = ["get", "ns", "-o", "json"]
    rc, out, err = run_oc_subcommand(
        context, subcmd, retries=retries, oc_timeout_seconds=oc_timeout_seconds
    )
    if rc != 0 or not out:
        return []
    try:
        obj = json.loads(out)
    except json.JSONDecodeError as e:
        logging.warning(f"Failed to parse namespaces JSON: {e}")
        return []
    # Collect namespace names and metadata for ephemeral detection
    namespaces_with_metadata = []
    for item in obj.get("items", []):
        metadata = item.get("metadata", {})
        ns_name = metadata.get("name")
        if ns_name:
            namespaces_with_metadata.append((ns_name, metadata))

    filtered: list[str] = []
    for ns_name, ns_metadata in namespaces_with_metadata:
        # Exclude ephemeral namespaces if enabled (check both labels and name patterns)
        if exclude_ephemeral and is_ephemeral_namespace(ns_name, ns_metadata):
            if _VERBOSE:
                print(color(f"  [skip ephemeral] {ns_name}", YELLOW))
            continue

        include = True
        if include_patterns:
            include = any(p.search(ns_name) for p in include_patterns)
        if not include:
            if _VERBOSE:
                print(color(f"  [skip include filter] {ns_name}", YELLOW))
            continue
        if exclude_patterns and any(p.search(ns_name) for p in exclude_patterns):
            if _VERBOSE:
                print(color(f"  [skip exclude filter] {ns_name}", YELLOW))
            continue
        filtered.append(ns_name)
    return filtered


# ---------------------------
# Save pod artifacts (describe + logs) into per-cluster directory under artifacts_root/<cluster>/
# Returns (description_path, log_path) as absolute paths
# ---------------------------
def save_pod_artifacts(
    context: str,
    cluster: str,
    namespace: str,
    pod: str,
    retries: int,
    oc_timeout_seconds: int,
    artifacts_root: Path,
) -> tuple[str, str]:
    """
    Save 'oc describe pod' and pod logs into files under artifacts_root/<cluster>/.
    Log file contains: first --previous (crashed container), then current logs, in one file.
    Filenames include namespace, pod name and timestamp to avoid collisions.
    Returns absolute file paths (description_file, pod_log_file).
    """
    ts = now_ts_for_filename()
    cluster_dir = (artifacts_root / cluster).resolve()
    cluster_dir.mkdir(parents=True, exist_ok=True)

    # safe filename parts
    ns_safe = re.sub(r"[^A-Za-z0-9_.-]", "_", namespace)
    pod_safe = re.sub(r"[^A-Za-z0-9_.-]", "_", pod)

    desc_fname = f"{ns_safe}__{pod_safe}__{ts}__desc.txt"
    log_fname = f"{ns_safe}__{pod_safe}__{ts}__log.txt"

    desc_path = cluster_dir / desc_fname
    log_path = cluster_dir / log_fname

    # oc describe pod
    try:
        rc, out, err = run_oc_subcommand(
            context,
            ["-n", namespace, "describe", "pod", pod],
            retries=retries,
            oc_timeout_seconds=oc_timeout_seconds,
        )
        content_desc = (
            out if rc == 0 and out else (err if err else "Failed to fetch pod description")
        )
    except Exception as e:
        logging.error(f"Error fetching pod description for {namespace}/{pod}: {e}")
        content_desc = f"Error fetching pod description: {e}"

    try:
        desc_path.write_text(content_desc)
    except Exception as e:
        # fallback to best-effort path
        desc_path = cluster_dir / f"{ns_safe}__{pod_safe}__{ts}__desc.failed.txt"
        with contextlib.suppress(Exception):
            desc_path.write_text(
                f"Failed to write description: {e}\nOriginal content:\n{content_desc}"
            )

    # oc logs: --previous first (crashed container), then current; append both to one file
    log_sections: list[str] = []
    try:
        # 1. Previous container logs (from the run that OOM'd/crashed)
        rc_prev, out_prev, err_prev = run_oc_subcommand(
            context,
            ["-n", namespace, "logs", pod, "--previous"],
            retries=retries,
            oc_timeout_seconds=oc_timeout_seconds,
        )
        prev_content = out_prev if rc_prev == 0 and out_prev else (err_prev or "(no previous logs)")
        log_sections.append(
            "=== Previous container logs (oc logs <pod> --previous) ===\n" + prev_content
        )
        # 2. Current container logs
        rc_cur, out_cur, err_cur = run_oc_subcommand(
            context,
            ["-n", namespace, "logs", pod],
            retries=retries,
            oc_timeout_seconds=oc_timeout_seconds,
        )
        cur_content = out_cur if rc_cur == 0 and out_cur else (err_cur or "(no current logs)")
        log_sections.append("=== Current container logs (oc logs <pod>) ===\n" + cur_content)
        log_content = "\n\n".join(log_sections)
    except Exception as e:
        logging.error(f"Error fetching logs for {namespace}/{pod}: {e}")
        log_content = f"Error fetching logs: {e}"

    try:
        log_path.write_text(log_content)
    except Exception as e:
        log_path = cluster_dir / f"{ns_safe}__{pod_safe}__{ts}__log.failed.txt"
        with contextlib.suppress(Exception):
            log_path.write_text(f"Failed to write logs: {e}\nOriginal logs content:\n{log_content}")

    return str(desc_path.resolve()), str(log_path.resolve())


def is_artifact_meaningful(content: str) -> bool:
    """
    Check if artifact content is meaningful (not just 'pod not found' errors).

    Uses a two-stage approach:
    1. Size check: If content is reasonably large (>2KB), assume it's meaningful
    2. Pattern check: For small content, look for specific 'oc' error patterns

    Returns:
        True if content has useful information, False if pod was deleted/not found
    """
    if not content or not content.strip():
        return False

    # Stage 1: Size-based heuristic
    # If content is larger than 2KB, it's likely meaningful (not just an error message)
    MEANINGFUL_SIZE_THRESHOLD = 2048  # 2KB
    content_size = len(content.encode("utf-8"))

    if content_size >= MEANINGFUL_SIZE_THRESHOLD:
        return True

    # Stage 2: Pattern matching for small content (< 2KB)
    # Only apply strict error pattern checks on small files
    lower = content.lower()
    lines = content.strip().split("\n")
    num_lines = len(lines)

    # Small files (< 10 lines) with specific 'oc' error patterns are likely not meaningful
    if num_lines < 10:
        # Check for exact "Error from server (NotFound): pods "..." not found" pattern
        if "error from server" in lower and "notfound" in lower and "pods" in lower:
            return False
        # Check for "Error from server: pods "..." not found" pattern
        if "error from server" in lower and "not found" in lower and "pods" in lower:
            return False
        # Check for standalone "pod not found" errors
        for line in lines:
            line_lower = line.lower().strip()
            if line_lower.startswith("error") and "pod" in line_lower and "not found" in line_lower:
                return False

    # If we got here, either:
    # - Content is between 10 lines and 2KB (likely meaningful)
    # - Or no error patterns matched
    return True


# ---------------------------
# namespace worker (oc-only)
# ---------------------------
def namespace_worker_oc(
    context: str,
    namespace: str,
    retries: int,
    oc_timeout_seconds: int,
    time_range_seconds: int | None = None,
) -> dict[str, dict[str, Any]] | None:
    """Process namespace to find OOMKilled and CrashLoopBackOff pods."""
    pod_map: dict[str, dict[str, Any]] = {}

    # OPTIMIZATION: Fetch events once instead of 3 separate API calls
    all_events = get_all_events_oc(
        context,
        namespace,
        retries=retries,
        oc_timeout_seconds=oc_timeout_seconds,
        time_range_seconds=time_range_seconds,
    )

    # Filter events in memory for OOMKilled, CrashLoop, and BackOff
    oom_events: list[dict[str, str]] = []
    crash_events: list[dict[str, str]] = []
    backoff_events: list[dict[str, str]] = []

    for ev in all_events:
        reason = ev.get("reason", "")
        reason_lower = reason.lower()
        involved = ev.get("involvedObject", {})
        # Only process Pod events — ignore Node, DaemonSet, Deployment, etc.
        # to prevent namespace misattribution (e.g. KONFLUX-14702).
        if involved.get("kind") != "Pod":
            continue
        pod = involved.get("name")
        ts = ev.get("eventTime") or ev.get("lastTimestamp") or ev.get("firstTimestamp")

        if not pod or not ts:
            continue

        event_data = {
            "pod": pod,
            "reason": reason,
            "timestamp": parse_timestamp_to_iso(ts),
        }

        if "oomkilled" in reason_lower:
            oom_events.append(event_data)
        elif "crashloop" in reason_lower:
            crash_events.append(event_data)
        elif reason_lower == "backoff":
            # Match only the exact Kubernetes "BackOff" restart reason (container
            # crash exponential back-off). The previous substring match
            # "backoff" in reason_lower also caught ImagePullBackOff and
            # ErrImageBackOff, which are image-pull failures — not crash loops —
            # causing false-positive CrashLoopBackOff reports (KONFLUX-13422).
            backoff_events.append(event_data)

    # Fetch pods once for both OOM/Crash detection and for
    # application/component labels (incl. event-only pods)
    pod_items = get_pods_items(context, namespace, retries, oc_timeout_seconds)
    labels_map: dict[str, tuple[str, str]] = {}
    for item in pod_items:
        name = item.get("metadata", {}).get("name")
        if name:
            labels_map[name] = _application_component_from_labels(
                item.get("metadata", {}).get("labels")
            )

    # Also check pod status directly for OOMKilled and CrashLoopBackOff (same time range)
    oom_pods = oomkilled_via_pods_oc(
        context,
        namespace,
        retries=retries,
        oc_timeout_seconds=oc_timeout_seconds,
        time_range_seconds=time_range_seconds,
        items=pod_items,
    )
    crash_pods = crashloop_via_pods_oc(
        context,
        namespace,
        retries=retries,
        oc_timeout_seconds=oc_timeout_seconds,
        time_range_seconds=time_range_seconds,
        items=pod_items,
    )

    for e in oom_events:
        p = e["pod"]
        pod_map.setdefault(
            p,
            {
                "pod": p,
                "oom_timestamps": [],
                "crash_timestamps": [],
                "sources": set(),
                "application": "",
                "component": "",
            },
        )
        pod_map[p]["oom_timestamps"].append(e.get("timestamp", ""))
        pod_map[p]["sources"].add("events")
        if p in labels_map:
            pod_map[p]["application"], pod_map[p]["component"] = labels_map[p]
    for e in crash_events + backoff_events:
        p = e["pod"]
        pod_map.setdefault(
            p,
            {
                "pod": p,
                "oom_timestamps": [],
                "crash_timestamps": [],
                "sources": set(),
                "application": "",
                "component": "",
            },
        )
        pod_map[p]["crash_timestamps"].append(e.get("timestamp", ""))
        pod_map[p]["sources"].add("events")
        if p in labels_map:
            pod_map[p]["application"], pod_map[p]["component"] = labels_map[p]
    # Add OOM pods found via pod status (they already have application/component in e)
    for e in oom_pods:
        p = e["pod"]
        pod_map.setdefault(
            p,
            {
                "pod": p,
                "oom_timestamps": [],
                "crash_timestamps": [],
                "sources": set(),
                "application": e.get("application", ""),
                "component": e.get("component", ""),
            },
        )
        pod_map[p]["oom_timestamps"].append(e.get("timestamp", ""))
        pod_map[p]["sources"].add("oc_get_pods")
        pod_map[p]["application"] = e.get("application", "") or pod_map[p].get("application", "")
        pod_map[p]["component"] = e.get("component", "") or pod_map[p].get("component", "")
    for e in crash_pods:
        p = e["pod"]
        pod_map.setdefault(
            p,
            {
                "pod": p,
                "oom_timestamps": [],
                "crash_timestamps": [],
                "sources": set(),
                "application": e.get("application", ""),
                "component": e.get("component", ""),
            },
        )
        pod_map[p]["crash_timestamps"].append(e.get("timestamp", ""))
        pod_map[p]["sources"].add("oc_get_pods")
        pod_map[p]["application"] = e.get("application", "") or pod_map[p].get("application", "")
        pod_map[p]["component"] = e.get("component", "") or pod_map[p].get("component", "")

    # Drop event-only pods that don't exist in the pod listing — they indicate
    # stale events or cross-namespace references that would cause namespace
    # misattribution in downstream reports (e.g. KONFLUX-14702).
    actual_pod_names = set(labels_map.keys())
    if pod_map and actual_pod_names:
        event_only = [
            p
            for p, info in pod_map.items()
            if info.get("sources", set()) == {"events"} and p not in actual_pod_names
        ]
        for p in event_only:
            del pod_map[p]

    if pod_map:
        out_ns: dict[str, dict[str, Any]] = {}
        for p, info in pod_map.items():
            out_ns[p] = {
                "pod": p,
                "oom_timestamps": sorted(list(set(info.get("oom_timestamps", [])))),
                "crash_timestamps": sorted(list(set(info.get("crash_timestamps", [])))),
                "sources": sorted(list(info.get("sources", []))),
                "application": info.get("application", ""),
                "component": info.get("component", ""),
            }
        return out_ns
    return None


# ---------------------------
# query a single cluster (namespaces in parallel batches)
# ---------------------------
def query_context(
    context: str,
    retries: int,
    oc_timeout_seconds: int,
    ns_batch_size: int = DEFAULT_NS_BATCH_SIZE,
    ns_workers: int = DEFAULT_NS_WORKERS,
    time_range_seconds: int | None = None,
    exclude_ephemeral: bool = True,
    artifacts_root: Path | None = None,
) -> tuple[str, dict[str, Any], str | None]:
    cluster = short_cluster_name(context)
    print(color(f"\n→ Processing cluster: {cluster}", BLUE))

    ok, msg = check_cluster_connectivity(
        context, retries=retries, oc_timeout_seconds=oc_timeout_seconds
    )
    if not ok:
        err_msg = f"Cluster {cluster} unreachable or auth/connectivity failure: {msg}"
        print(color(f"  [SKIP] {err_msg}", RED))
        return cluster, {}, err_msg

    # Access global patterns (set in parse_args)
    namespaces = get_namespaces_for_context(
        context,
        retries=retries,
        oc_timeout_seconds=oc_timeout_seconds,
        include_patterns=_INCLUDE_PATTERNS,
        exclude_patterns=_EXCLUDE_PATTERNS,
        exclude_ephemeral=exclude_ephemeral,
    )
    if not namespaces:
        return cluster, {}, None

    if _VERBOSE:
        print(color(f"  Will scan {len(namespaces)} namespaces:", BLUE))
        for ns in namespaces:
            print(color(f"    {ns}", BLUE))

    cluster_result: dict[str, Any] = {}

    total_ns = len(namespaces)
    for i in range(0, total_ns, ns_batch_size):
        ns_batch = namespaces[i : i + ns_batch_size]
        print(
            color(
                f"  Namespace batch {i // ns_batch_size + 1}: {len(ns_batch)} namespaces",
                YELLOW,
            )
        )
        if _VERBOSE:
            print(color(f"    Scanning: {', '.join(ns_batch)}", BLUE))

        with ThreadPoolExecutor(max_workers=min(ns_workers, len(ns_batch))) as ex:
            futures = {
                ex.submit(
                    namespace_worker_oc,
                    context,
                    ns,
                    retries,
                    oc_timeout_seconds,
                    time_range_seconds,
                ): ns
                for ns in ns_batch
            }
            for fut in as_completed(futures):
                ns = futures[fut]
                try:
                    res = fut.result()
                    if res:
                        # Save artifacts for each pod found in this namespace
                        out_ns_with_artifacts: dict[str, dict[str, Any]] = {}
                        skipped = 0
                        for p, info in res.items():
                            if artifacts_root is not None:
                                desc_file, log_file = save_pod_artifacts(
                                    context,
                                    cluster,
                                    ns,
                                    p,
                                    retries,
                                    oc_timeout_seconds,
                                    artifacts_root=artifacts_root,
                                )
                                # Validate artifacts - skip if pod was deleted/not found
                                try:
                                    desc_content = Path(desc_file).read_text()
                                    log_content = Path(log_file).read_text()
                                    if not is_artifact_meaningful(
                                        desc_content
                                    ) or not is_artifact_meaningful(log_content):
                                        Path(desc_file).unlink(missing_ok=True)
                                        Path(log_file).unlink(missing_ok=True)
                                        skipped += 1
                                        continue
                                except Exception:  # nosec B110
                                    pass  # Keep pod if validation fails
                                info["description_file"] = desc_file
                                info["pod_log_file"] = log_file
                            else:
                                info["description_file"] = ""
                                info["pod_log_file"] = ""
                            out_ns_with_artifacts[p] = info
                        if out_ns_with_artifacts:
                            cluster_result[ns] = out_ns_with_artifacts
                        msg = f"    Namespace {ns}: {len(out_ns_with_artifacts)} pod(s) kept"
                        if skipped > 0:
                            msg += f" ({skipped} skipped - pod deleted)"
                        print(color(msg, YELLOW))
                except Exception as e:
                    print(color(f"    Error processing namespace {ns}: {e}", RED))

    # write per-cluster log under artifacts_root/<cluster>/ if available
    if artifacts_root:
        try:
            cluster_dir = (artifacts_root / cluster).resolve()
            cluster_dir.mkdir(parents=True, exist_ok=True)
            outfile = cluster_dir / f"{cluster}.log"
            outfile.write_text(json.dumps(cluster_result, indent=2))
        except Exception:  # nosec B110
            pass

    return cluster, cluster_result, None


# ---------------------------
# cluster batch runner
# ---------------------------
def run_batches(
    contexts: list[str],
    batch_size: int,
    retries: int,
    oc_timeout_seconds: int,
    ns_batch_size: int,
    ns_workers: int,
    time_range_seconds: int | None = None,
    exclude_ephemeral: bool = True,
    output_dir: Path | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """
    Run cluster processing with constant parallelism.

    Instead of processing in fixed batches, maintains constant parallelism:
    when one cluster finishes, immediately start the next one.
    """
    artifacts_root = (
        output_dir if output_dir is not None else Path("output")
    ).resolve() / "logs_and_description_files"
    results: dict[str, Any] = {}
    skipped: dict[str, str] = {}
    total = len(contexts)
    context_index = 0
    active_futures: dict[Any, str] = {}

    with ThreadPoolExecutor(max_workers=batch_size) as ex:
        # Start initial batch
        while context_index < total and len(active_futures) < batch_size:
            ctx = contexts[context_index]
            context_index += 1
            fut = ex.submit(
                query_context,
                ctx,
                retries,
                oc_timeout_seconds,
                ns_batch_size,
                ns_workers,
                time_range_seconds,
                exclude_ephemeral,
                artifacts_root,
            )
            active_futures[fut] = ctx
            print(
                color(
                    f"Started processing cluster: {short_cluster_name(ctx)}",
                    BLUE,
                )
            )

        # Process as they complete, starting new ones to maintain parallelism
        while active_futures:
            for fut in as_completed(active_futures):
                ctx = active_futures.pop(fut)
                try:
                    cluster, data, err = fut.result()
                    if err:
                        skipped[cluster] = err
                        print(color(f"Skipped cluster {cluster}: {err}", RED))
                    else:
                        results[cluster] = data
                        print(color(f"Completed cluster {cluster}", GREEN))
                except Exception as e:
                    cluster_guess = short_cluster_name(ctx)
                    skipped[cluster_guess] = str(e)
                    print(color(f"Error processing {cluster_guess}: {e}", RED))

                # Start next cluster if available
                if context_index < total:
                    next_ctx = contexts[context_index]
                    context_index += 1
                    next_fut = ex.submit(
                        query_context,
                        next_ctx,
                        retries,
                        oc_timeout_seconds,
                        ns_batch_size,
                        ns_workers,
                        time_range_seconds,
                        exclude_ephemeral,
                        artifacts_root,
                    )
                    active_futures[next_fut] = next_ctx
                    print(
                        color(
                            f"Started processing cluster: {short_cluster_name(next_ctx)}",
                            BLUE,
                        )
                    )

    return results, skipped


# ---------------------------
# output directory management
# ---------------------------
def ensure_output_directory(path_str: str = "output") -> Path:
    """
    Ensure the output subdirectory exists, creating it if necessary.
    Also ensures the tarballs/ subdir exists (for oom_logs_and_desc_bundle_generator
    when run from Jenkins or locally, so tarballs do not clutter the main output dir).

    Uses mkdir(parents=True, exist_ok=True) (equivalent to 'mkdir -p'): creates
    dirs only when missing; never removes or truncates existing dirs, so historical
    data is preserved across runs.

    Returns:
        Path to the output directory
    """
    output_dir = Path(path_str)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "tarballs").mkdir(parents=True, exist_ok=True)
    return output_dir


def move_existing_output_files(target_dir: Path) -> int:
    """
    Move all existing output files (oom_results.* and timestamped versions)
    from current directory to target directory.

    Returns:
        Number of files moved
    """
    output_dir = target_dir
    # Ensure it exists (mkdir -p style: create if missing, never truncate existing)
    output_dir.mkdir(parents=True, exist_ok=True)
    moved_count = 0

    # Pattern to match output files
    output_patterns = [
        "oom_results.csv",
        "oom_results.json",
        "oom_results.html",
        "oom_results.table",
        "oom_results_*.csv",
        "oom_results_*.json",
        "oom_results_*.html",
        "oom_results_*.table",
    ]

    current_dir = Path(".")
    for pattern in output_patterns:
        # Handle wildcard patterns
        if "*" in pattern:
            files = glob.glob(str(current_dir / pattern))
        else:
            files = [str(current_dir / pattern)] if (current_dir / pattern).exists() else []

        for file_path_str in files:
            file_path = Path(file_path_str)
            if file_path.exists() and file_path.is_file():
                try:
                    dest_path = output_dir / file_path.name

                    # If source and destination are the same (e.g. output_dir is current dir), skip
                    if file_path.resolve() == dest_path.resolve():
                        continue

                    # If file already exists in output dir, skip (don't overwrite)
                    if not dest_path.exists():
                        file_path.rename(dest_path)
                        moved_count += 1
                    else:
                        # If destination exists, rename with timestamp
                        # from file's last modified time
                        timestamp = timestamp_for_backup_from_file(file_path)
                        suffix = file_path.suffix
                        stem = file_path.stem
                        backup_name = f"{stem}_{timestamp}{suffix}"
                        dest_path = output_dir / backup_name
                        file_path.rename(dest_path)
                        moved_count += 1
                except Exception as e:
                    logging.warning(f"Failed to move {file_path} to output directory: {e}")

    if moved_count > 0:
        print(color(f"Moved {moved_count} existing output file(s) to 'output' directory", YELLOW))

    return moved_count


# ---------------------------
# file backup utilities
# ---------------------------
def backup_existing_file(file_path: Path) -> Path | None:
    """Backup an existing file by renaming it with a timestamp.

    Args:
        file_path: Path to the file to backup

    Returns:
        Path to the backup file if backup was successful, None otherwise
    """
    if not file_path.exists():
        return None

    try:
        # Use file's last modified time for backup name (same format: DD-MMM-YYYY_HH-MM-SS-TZ)
        timestamp = timestamp_for_backup_from_file(file_path)
        suffix = file_path.suffix
        stem = file_path.stem
        backup_name = f"{stem}_{timestamp}{suffix}"
        backup_path = file_path.parent / backup_name

        # Rename the file
        file_path.rename(backup_path)
        return backup_path
    except Exception as e:
        logging.warning(f"Failed to backup {file_path}: {e}")
        return None


def backup_output_files(
    json_path: Path,
    csv_path: Path,
    table_path: Path,
    html_path: Path,
) -> None:
    """Backup existing output files before generating new ones."""
    backups = []

    for file_path in [json_path, csv_path, table_path, html_path]:
        backup_path = backup_existing_file(file_path)
        if backup_path:
            backups.append(backup_path)

    if backups:
        print(color(f"\nBacked up {len(backups)} existing file(s):", YELLOW))
        for backup_path in backups:
            print(color(f"  → {backup_path.name}", YELLOW))


# ---------------------------
# exports & pretty print
# ---------------------------
def collect_rows(results: dict[str, Any], time_range_str: str = "1d") -> list[dict[str, str]]:
    """
    Collect all rows from results dictionary.

    Returns a list of dictionaries representing rows, sorted by type
    (OOMKilled first, then CrashLoopBackOff).
    """
    rows = []
    # Skip _metadata if present
    for cluster, ns_map in results.items():
        if cluster == "_metadata":
            continue
        for ns, pods in ns_map.items():
            for pod_name, info in pods.items():
                desc = info.get("description_file", "")
                plog = info.get("pod_log_file", "")
                sources = ";".join(info.get("sources", [])) if info.get("sources") else ""
                application = info.get("application", "")
                component = info.get("component", "")
                # OOM rows
                if info.get("oom_timestamps"):
                    rows.append(
                        {
                            "cluster": cluster,
                            "namespace": ns,
                            "pod": pod_name,
                            "type": "OOMKilled",
                            "application": application,
                            "component": component,
                            "timestamps": ";".join(info.get("oom_timestamps")),
                            "sources": sources,
                            "description_file": desc,
                            "pod_log_file": plog,
                            "time_range": time_range_str,
                        }
                    )
                # Crash rows
                if info.get("crash_timestamps"):
                    rows.append(
                        {
                            "cluster": cluster,
                            "namespace": ns,
                            "pod": pod_name,
                            "type": "CrashLoopBackOff",
                            "application": application,
                            "component": component,
                            "timestamps": ";".join(info.get("crash_timestamps")),
                            "sources": sources,
                            "description_file": desc,
                            "pod_log_file": plog,
                            "time_range": time_range_str,
                        }
                    )

    # Sort: OOMKilled first, then CrashLoopBackOff
    def sort_key(row: dict[str, str]) -> tuple[int, str, str, str]:
        type_val = row.get("type", "")
        if type_val == "OOMKilled":
            return (
                0,
                row.get("cluster", ""),
                row.get("namespace", ""),
                row.get("pod", ""),
            )
        elif type_val == "CrashLoopBackOff":
            return (
                1,
                row.get("cluster", ""),
                row.get("namespace", ""),
                row.get("pod", ""),
            )
        else:
            return (
                2,
                row.get("cluster", ""),
                row.get("namespace", ""),
                row.get("pod", ""),
            )

    rows.sort(key=sort_key)
    return rows


def export_table(rows: list[dict[str, str]], table_path: Path) -> None:
    """Export rows to a table-formatted file."""
    if not rows:
        return

    columns = [
        "cluster",
        "namespace",
        "pod",
        "type",
        "application",
        "component",
        "timestamps",
        "sources",
        "description_file",
        "pod_log_file",
        "time_range",
    ]

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(row.get(col, "")))

    # Generate table
    lines = []

    # Build header row first to calculate exact width
    header_parts = [f" {col:<{widths[col]}} " for col in columns]
    header_row = "|" + "|".join(header_parts) + "|"

    # Calculate total width: length of the header row
    total_width = len(header_row)

    # Header separator (continuous line of dashes matching table width)
    header_sep = "-" * total_width
    lines.append(header_sep)

    # Header row
    lines.append(header_row)

    # Header separator again
    lines.append(header_sep)

    # Data rows
    for row in rows:
        data_parts = [f" {row.get(col, ''):<{widths[col]}} " for col in columns]
        data_row = "|" + "|".join(data_parts) + "|"
        lines.append(data_row)

    # Footer separator
    lines.append(header_sep)

    # Write to file
    try:
        table_path.write_text("\n".join(lines))
        print(color(f"Table written → {table_path}", GREEN))
    except OSError as e:
        logging.error(f"Failed to write table file {table_path}: {e}")
        print(color(f"ERROR: Failed to write table file: {e}", RED))


def export_results(
    results: dict[str, Any],
    json_path: Path,
    csv_path: Path,
    table_path: Path,
    html_path: Path | None = None,
    time_range_str: str = "1d",
    output_dir: Path | None = None,
    plot_range_seconds: int | None = None,
    plot_range_str: str = "2M",
) -> None:
    """Export results to JSON, CSV, TABLE, and HTML files."""
    # Collect and sort rows
    rows = collect_rows(results, time_range_str)

    # Export JSON
    results_with_metadata = results.copy()
    results_with_metadata["_metadata"] = {"time_range": time_range_str}
    try:
        json_path.write_text(json.dumps(results_with_metadata, indent=2))
        print(color(f"JSON written → {json_path}", GREEN))
    except OSError as e:
        logging.error(f"Failed to write JSON file {json_path}: {e}")
        print(color(f"ERROR: Failed to write JSON file: {e}", RED))

    # Export CSV
    try:
        with csv_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "cluster",
                    "namespace",
                    "pod",
                    "type",
                    "application",
                    "component",
                    "timestamps",
                    "sources",
                    "description_file",
                    "pod_log_file",
                    "time_range",
                ]
            )
            for row in rows:
                writer.writerow(
                    [
                        row["cluster"],
                        row["namespace"],
                        row["pod"],
                        row["type"],
                        row.get("application", ""),
                        row.get("component", ""),
                        row["timestamps"],
                        row["sources"],
                        row["description_file"],
                        row["pod_log_file"],
                        row["time_range"],
                    ]
                )
        print(color(f"CSV written → {csv_path}", GREEN))
    except OSError as e:
        logging.error(f"Failed to write CSV file {csv_path}: {e}")
        print(color(f"ERROR: Failed to write CSV file: {e}", RED))

    # Export TABLE
    export_table(rows, table_path)

    # Export HTML (with optional historical graph)
    if html_path and generate_html_report:
        try:
            historical_series = None
            historical_series_by_cluster = {}
            historical_html_links = []
            if output_dir is not None and plot_range_seconds is not None:
                historical_series = build_historical_series_from_output_dir(
                    output_dir, plot_range_seconds
                )
                historical_series_by_cluster = build_historical_series_by_cluster_from_output_dir(
                    output_dir, plot_range_seconds
                )
            if output_dir is not None:
                historical_html_links = get_historical_html_links(output_dir)
            generate_html_report(
                rows,
                time_range_str,
                html_path,
                report_generated_est=report_generated_est(),
                historical_series=historical_series,
                historical_series_by_cluster=historical_series_by_cluster,
                historical_html_links=historical_html_links,
                plot_range_str=plot_range_str,
            )
            print(color(f"HTML written → {html_path}", GREEN))
        except Exception as e:
            logging.error(f"Failed to write HTML file {html_path}: {e}")
            print(color(f"ERROR: Failed to write HTML file: {e}", RED))
    elif html_path and not generate_html_report:
        logging.warning("HTML export module not available, skipping HTML generation")
        print(color("WARNING: HTML export module not available, skipping HTML generation", YELLOW))


def pretty_print(results: dict[str, Any], skipped: dict[str, str]) -> None:
    for cluster, ns_map in results.items():
        print()
        print(color(f"Cluster: {cluster}", BLUE))
        if not ns_map:
            print(color("  (no namespaces with OOM/CrashLoopBackOff found)", GREEN))
            continue
        for ns, pods in ns_map.items():
            print(color(f"  Namespace: {ns}", YELLOW))
            for pod_name, info in pods.items():
                heading_color = (
                    RED if (info.get("oom_timestamps") or info.get("crash_timestamps")) else GREEN
                )
                print(color(f"    Pod: {pod_name}", heading_color))
                if info.get("oom_timestamps"):
                    for t in info["oom_timestamps"]:
                        print(f"      - OOMKilled at: {t}")
                if info.get("crash_timestamps"):
                    for t in info["crash_timestamps"]:
                        print(f"      - CrashLoopBackOff event at: {t}")
                if not info.get("oom_timestamps") and not info.get("crash_timestamps"):
                    sources_str = ", ".join(info.get("sources", []))
                    print(f"      - Detected (no timestamps) via sources: {sources_str}")
                # print artifacts paths
                if info.get("description_file") or info.get("pod_log_file"):
                    print(f"      - description_file: {info.get('description_file', '')}")
                    print(f"      - pod_log_file: {info.get('pod_log_file', '')}")
    if skipped:
        print()
        print(color("Skipped / Unreachable clusters:", RED))
        for c, msg in skipped.items():
            print(color(f"  {c}: {msg}", RED))


def _pod_base_name(full_name: str) -> str:
    """
    Derive a readable, collatable base pod name by replacing hashes and random IDs
    with '*', so multiple pods group under one report (e.g. CI jobs, hostnames).
    Examples:
      backfill-redis-v1-2-on-pull-request-g6w9f-run-unit-test -> ...-*-run-unit-test
      kube-rbac-proxy-crio-ip-10-202-25-219.ec2.internal -> ...-ip-*.internal
      instance-6xsb9 -> instance
      gatekeeper-op41130a155... -> gatekeeper-op
    """
    if not full_name:
        return full_name
    name = full_name

    # 1. Hostname: *-ip-<something>.ec2.internal or *.internal -> *-ip-*.internal
    if ".internal" in name and "-ip-" in name:
        idx = name.find("-ip-")
        inr = name.find(".internal")
        if idx >= 0 and inr > idx:
            name = name[: idx + 4] + "*" + name[inr:]

    # 2. instance-<short single segment> -> instance
    if name.startswith("instance-"):
        rest = name[len("instance-") :]
        if rest.isalnum() and len(rest) <= 10 and "-" not in rest:
            return "instance"

    # 3. Split by '-' for segment-wise rules (rejoin later)
    segments = name.split("-")
    out: list[str] = []

    for _i, seg in enumerate(segments):
        if not seg:
            out.append(seg)
            continue
        # Segment with a dot (e.g. 219.ec2.internal) - keep as-is or already handled
        if "." in seg:
            out.append(seg)
            continue
        # Short word + long alnum (e.g. op41130..., observ1b4c..., pullca107...)
        # -> word only (check before long-hash)
        if len(seg) > 10 and seg.isalnum():
            # Try known CI/word prefixes first (so "pull" wins over
            # "pullca"); no "pu" so pu<hash> -> *
            for prefix in ("pull", "reque", "observ", "op", "midstream", "on"):
                if seg.startswith(prefix) and len(seg) > len(prefix) + 12:
                    out.append(prefix)
                    break
            else:
                # Longest all-alpha prefix followed by 12+ chars (the hash)
                word_len = 0
                for j, c in enumerate(seg):
                    if c.isalpha():
                        word_len = j + 1
                    else:
                        break
                if word_len >= 2 and word_len < len(seg) and len(seg) - word_len >= 12:
                    word = seg[:word_len]
                    # "pu" + long hash -> * so trailing -* gets
                    # dropped (e.g. cloudwatch-aggregator-on)
                    if word == "pu" and len(seg) - word_len >= 20:
                        out.append("*")
                    else:
                        out.append(word)
                elif len(seg) >= 20:
                    # No alpha prefix (e.g. t98022b86..., a08677e97...); treat as hash
                    out.append("*")
                else:
                    out.append(seg)
            continue
        # Long hash segment (20+ alnum) with no alpha prefix -> *
        if len(seg) >= 20 and seg.isalnum():
            out.append("*")
            continue
        # Short random-looking ID (5-8 alnum, contains digit) -> *
        if 5 <= len(seg) <= 8 and seg.isalnum() and any(c.isdigit() for c in seg):
            out.append("*")
            continue
        # ReplicaSet-style hash (8-10 alnum, contains digit) as standalone segment -> *
        if 8 <= len(seg) <= 10 and seg.isalnum() and any(c.isdigit() for c in seg):
            out.append("*")
            continue
        out.append(seg)

    # 4. Collapse consecutive '*' into one
    collapsed: list[str] = []
    for s in out:
        if s == "*" and collapsed and collapsed[-1] == "*":
            continue
        collapsed.append(s)
    result = "-".join(collapsed)

    # 5. Drop trailing lone '*' or *-only suffix (e.g. odh-midstream-* -> odh-midstream)
    while result.endswith("-*") and result.count("-") > 1:
        result = result[:-2]

    # 6. Classic ReplicaSet: <name>-<hash>-<suffix> if we still have *-* at end, keep one *
    if result.endswith("-*-*"):
        result = result[:-2]  # remove last -*

    # 7. Trailing "-pod" (CI job pod suffix)
    if result.endswith("-pod") and result.count("-") > 1:
        result = result[:-4]

    # 8. Trailing short random-looking segment (5-8 alnum, e.g. -wzpwf, -bjcvs)
    # -> * (keep words like verify, apply)
    _keep_trailing = frozenset(
        (
            "verify",
            "apply",
            "build",
            "push",
            "pull",
            "scan",
            "test",
            "tags",
            "pod",
            "run",
            "tekton",
            "check",
            "observ",
            "dependencies",
            "unicode",
        )
    )
    while result.count("-") >= 1:
        last_part = result.rsplit("-", 1)[-1]
        if (
            5 <= len(last_part) <= 8
            and last_part.isalnum()
            and last_part.lower() not in _keep_trailing
        ):
            result = result[: -len(last_part) - 1] + "-*"
            while result.endswith("-*") and result.count("-") > 1:
                result = result[:-2]
            break
        break

    return result if result else full_name


def _match_string_for_bundle_generator(pod_names: list[str]) -> str:
    """
    Return a substring that matches all given pod names in CSV column 3.
    oom_logs_and_desc_bundle_generator uses index($3, pod) > 0, so we need a
    literal string that appears in the actual pod names (not the display base name
    with asterisks). Use longest common prefix so we match exactly this group.
    """
    if not pod_names:
        return ""
    if len(pod_names) == 1:
        return pod_names[0]
    prefix = pod_names[0]
    for name in pod_names[1:]:
        i = 0
        for a, b in zip(prefix, name, strict=False):
            if a != b:
                break
            i += 1
        prefix = prefix[:i]
    # Strip trailing hyphen so we match "apiserver-69cc49fdf9" in "apiserver-69cc49fdf9-cbnj4"
    return prefix.rstrip("-") if prefix else pod_names[0]


def _date_from_timestamped_csv_basename(basename: str) -> str | None:
    """Extract DD-Mon-YYYY from oom_results_DD-Mon-YYYY_*.csv. Returns None if not matched."""
    if not basename.startswith("oom_results_") or not basename.endswith(".csv"):
        return None
    # oom_results_03-Feb-2026_12-04-19-EDT.csv -> 03-Feb-2026
    m = re.match(r"oom_results_(\d{2}-[A-Za-z]{3}-\d{4})_[^.]*\.csv", basename)
    return m.group(1) if m else None


def _label_from_timestamped_csv_basename(basename: str) -> str | None:
    """Extract display label DD-Mon-YYYY HH:MM from oom_results_DD-Mon-YYYY_HH-MM-SS-TZ.csv."""
    if not basename.startswith("oom_results_") or not basename.endswith(".csv"):
        return None
    # oom_results_03-Feb-2026_12-04-19-EDT.csv -> 03-Feb-2026 12:04
    m = re.match(
        r"oom_results_(\d{2}-[A-Za-z]{3}-\d{4})_(\d{2})-(\d{2})-(\d{2})-[^.]*\.csv", basename
    )
    if not m:
        return _date_from_timestamped_csv_basename(basename)  # fallback to date only
    return f"{m.group(1)} {m.group(2)}:{m.group(3)}"


# Month abbreviation to number (locale-independent for DD-Mon-YYYY in filenames)
_MONTH_ABBR_TO_NUM = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _run_date_from_timestamped_csv_basename(basename: str) -> date | None:
    """Parse run date (DD-Mon-YYYY) from filename to a date.

    For filtering/sorting. Locale-independent.
    """
    date_str = _date_from_timestamped_csv_basename(basename)
    if not date_str:
        return None
    try:
        # Locale-independent: DD-Mon-YYYY (e.g. 22-Jan-2026)
        parts = date_str.split("-")
        if len(parts) != 3:
            return None
        dd = int(parts[0])
        mon = _MONTH_ABBR_TO_NUM.get(parts[1].lower())
        yyyy = int(parts[2])
        if mon is None or dd < 1 or dd > 31 or yyyy < 2000 or yyyy > 2100:
            return None
        return date(yyyy, mon, dd)
    except (ValueError, TypeError):
        return None


def build_historical_series_from_output_dir(
    output_dir: Path,
    plot_range_seconds: int,
) -> list[tuple[str, int, int]]:
    """
    Build historical (label, oom_count, crash_count) from timestamped CSVs in output_dir,
    plus the current run from oom_results.csv if present (so today's run appears on the graph).
    Uses run date from filename (DD-Mon-YYYY); fallback to file mtime date if parse fails.
    Cutoff is relative to the **latest run in the directory** (not "now"). Sorted by run date.
    """
    resolved_dir = output_dir.resolve()
    series: list[tuple[str, int, int, date]] = []
    # 1) Timestamped backup CSVs
    files = sorted(resolved_dir.glob("oom_results_*_*.csv"))
    for path in files:
        try:
            run_date = _run_date_from_timestamped_csv_basename(path.name)
            if run_date is None:
                run_date = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
            label = _label_from_timestamped_csv_basename(path.name) or path.name
            oom, crash = 0, 0
            with path.open(newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    t = _normalize_type(row.get("type", ""))
                    if t == "OOMKilled":
                        oom += 1
                    elif t == "CrashLoopBackOff":
                        crash += 1
            series.append((label, oom, crash, run_date))
        except (OSError, csv.Error) as e:
            logging.debug(f"Skip {path.name}: {e}")
            continue
    # 2) Current run (oom_results.csv) so today's run appears on the graph
    main_csv = resolved_dir / "oom_results.csv"
    if main_csv.is_file():
        try:
            mtime = main_csv.stat().st_mtime
            run_date = datetime.fromtimestamp(mtime, tz=UTC).date()
            label = datetime.fromtimestamp(mtime).strftime(
                "%d-%b-%Y %H:%M"
            )  # local time for display
            oom, crash = 0, 0
            with main_csv.open(newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    t = _normalize_type(row.get("type", ""))
                    if t == "OOMKilled":
                        oom += 1
                    elif t == "CrashLoopBackOff":
                        crash += 1
            series.append((label, oom, crash, run_date))
        except (OSError, csv.Error) as e:
            logging.debug(f"Skip {main_csv.name}: {e}")
    if not series:
        return []
    # Cutoff relative to latest run in this directory (avoids dependence on system clock)
    latest = max(run_d for (_, _, _, run_d) in series)
    cutoff_ts = (
        datetime.combine(latest, datetime.min.time()).replace(tzinfo=UTC).timestamp()
        - plot_range_seconds
    )
    cutoff_date = datetime.fromtimestamp(cutoff_ts, tz=UTC).date()
    series = [
        (label, oom, crash, run_d) for (label, oom, crash, run_d) in series if run_d >= cutoff_date
    ]
    series.sort(key=lambda x: (x[3], x[0]))
    return [(label, oom, crash) for label, oom, crash, _ in series]


def build_historical_series_by_cluster_from_output_dir(
    output_dir: Path,
    plot_range_seconds: int,
) -> dict[str, list[tuple[str, int, int]]]:
    """
    Build per-cluster historical (label, oom_count, crash_count) from timestamped CSVs.
    Same cutoff logic as build_historical_series_from_output_dir. Returns dict cluster -> list
    of (label, oom, crash) for runs in plot range where that cluster had data.
    """
    resolved_dir = output_dir.resolve()
    files = sorted(resolved_dir.glob("oom_results_*_*.csv"))
    # Collect per (run_date, label) per-cluster counts
    run_data: list[tuple[str, date, dict[str, tuple[int, int]]]] = []
    for path in files:
        try:
            run_date = _run_date_from_timestamped_csv_basename(path.name)
            if run_date is None:
                run_date = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
            label = _label_from_timestamped_csv_basename(path.name) or path.name
            cluster_counts: dict[str, tuple[int, int]] = {}
            with path.open(newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cluster = (row.get("cluster") or "").strip() or "unknown"
                    if cluster not in cluster_counts:
                        cluster_counts[cluster] = (0, 0)
                    oom, crash = cluster_counts[cluster]
                    t = _normalize_type(row.get("type", ""))
                    if t == "OOMKilled":
                        oom += 1
                    elif t == "CrashLoopBackOff":
                        crash += 1
                    cluster_counts[cluster] = (oom, crash)
            run_data.append((label, run_date, cluster_counts))
        except (OSError, csv.Error) as e:
            logging.debug(f"Skip {path.name}: {e}")
            continue
    # Include current run (oom_results.csv) so today's run appears on per-cluster graphs
    main_csv = resolved_dir / "oom_results.csv"
    if main_csv.is_file():
        try:
            mtime = main_csv.stat().st_mtime
            run_date = datetime.fromtimestamp(mtime, tz=UTC).date()
            label = datetime.fromtimestamp(mtime).strftime(
                "%d-%b-%Y %H:%M"
            )  # local time for display
            cluster_counts = {}
            with main_csv.open(newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cluster = (row.get("cluster") or "").strip() or "unknown"
                    if cluster not in cluster_counts:
                        cluster_counts[cluster] = (0, 0)
                    oom, crash = cluster_counts[cluster]
                    t = _normalize_type(row.get("type", ""))
                    if t == "OOMKilled":
                        oom += 1
                    elif t == "CrashLoopBackOff":
                        crash += 1
                    cluster_counts[cluster] = (oom, crash)
            run_data.append((label, run_date, cluster_counts))
        except (OSError, csv.Error) as e:
            logging.debug(f"Skip {main_csv.name}: {e}")
    if not run_data:
        return {}
    latest = max(rd[1] for rd in run_data)
    cutoff_ts = (
        datetime.combine(latest, datetime.min.time()).replace(tzinfo=UTC).timestamp()
        - plot_range_seconds
    )
    cutoff_date = datetime.fromtimestamp(cutoff_ts, tz=UTC).date()
    # Build cluster -> list of (label, oom, crash) for runs in range
    by_cluster: dict[str, list[tuple[str, int, int, date]]] = {}
    for label, run_date, cluster_counts in run_data:
        if run_date < cutoff_date:
            continue
        for cluster, (oom, crash) in cluster_counts.items():
            if cluster not in by_cluster:
                by_cluster[cluster] = []
            by_cluster[cluster].append((label, oom, crash, run_date))
    for cluster in by_cluster:
        by_cluster[cluster].sort(key=lambda x: (x[3], x[0]))
    return {
        cluster: [(label, oom, crash) for label, oom, crash, _ in by_cluster[cluster]]
        for cluster in by_cluster
    }


def get_historical_html_links(output_dir: Path) -> list[tuple[str, str]]:
    """
    Return list of (label, filename) for timestamped oom_results_*_*.html in output_dir,
    sorted by run date descending (most recent first). Use relative filename so links work with file://.
    """
    resolved_dir = output_dir.resolve()
    candidates: list[tuple[date, str, str]] = []
    for path in resolved_dir.glob("oom_results_*_*.html"):
        # Reuse CSV basename helpers by pretending .html is .csv for date/label parsing
        fake_csv_name = path.name.replace(".html", ".csv")
        run_date = _run_date_from_timestamped_csv_basename(fake_csv_name)
        if run_date is None:
            run_date = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()
        label = _label_from_timestamped_csv_basename(fake_csv_name) or path.stem
        candidates.append((run_date, label, path.name))
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [(label, filename) for (_, label, filename) in candidates]


def _normalize_type(t: str) -> str:
    """Normalize type to OOMKilled or CrashLoopBackOff."""
    u = (t or "").strip().lower()
    if u == "oomkilled":
        return "OOMKilled"
    if u == "crashloopbackoff":
        return "CrashLoopBackOff"
    return (t or "").strip()


def _read_csv_rows_with_date(csv_path: Path, date_str: str) -> list[dict[str, str]]:
    """Read CSV and return list of row dicts with all columns (cluster, namespace, pod, type,
    application, component, timestamps, sources, description_file, pod_log_file, time_range, date).
    Preserves full row so HTML/details table and summaries get all fields. Old CSVs without
    application/component columns get empty strings."""
    rows: list[dict[str, str]] = []
    try:
        with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pod = (row.get("pod") or "").strip()
                if not pod:
                    continue
                raw_type = (row.get("type") or "").strip()
                out = {
                    "cluster": (row.get("cluster") or "").strip(),
                    "namespace": (row.get("namespace") or "").strip(),
                    "pod": pod,
                    "type": _normalize_type(raw_type),
                    "application": (row.get("application") or "").strip(),
                    "component": (row.get("component") or "").strip(),
                    "timestamps": (row.get("timestamps") or "").strip(),
                    "sources": (row.get("sources") or "").strip(),
                    "description_file": (row.get("description_file") or "").strip(),
                    "pod_log_file": (row.get("pod_log_file") or "").strip(),
                    "time_range": (row.get("time_range") or "").strip(),
                    "date": date_str,
                }
                rows.append(out)
    except OSError as e:
        logging.warning(f"Failed to read CSV {csv_path}: {e}")
    return rows


def _load_historical_rows_from_output_dir(output_dir: Path) -> list[dict[str, str]]:
    """Load rows from all timestamped oom_results_*_*.csv in output_dir (date from filename)."""
    historical: list[dict[str, str]] = []
    for path in sorted(output_dir.glob("oom_results_*_*.csv")):
        date_str = _date_from_timestamped_csv_basename(path.name)
        if date_str:
            historical.extend(_read_csv_rows_with_date(path, date_str))
    return historical


def _cleanup_codeowners_temp_dir() -> None:
    global _CODEOWNERS_TEMP_DIR
    if _CODEOWNERS_TEMP_DIR:
        shutil.rmtree(_CODEOWNERS_TEMP_DIR, ignore_errors=True)
        _CODEOWNERS_TEMP_DIR = None


def resolve_codeowners_dir(cli_arg: str | None) -> Path | None:
    """
    If cli_arg points to an existing directory, use it as konflux-release-data.
    Otherwise shallow-clone KONFLUX_RELEASE_DATA_REPO to a temp directory (cleaned on exit)
    and return that path. Returns None if clone fails or cli_arg is a non-directory path.
    """
    global _CODEOWNERS_TEMP_DIR, _CODEOWNERS_ATEXIT_REGISTERED

    if cli_arg and cli_arg.strip():
        p = Path(cli_arg.strip()).expanduser().resolve()
        if p.is_dir():
            return p
        print(color(f"WARNING: --codeowners-dir is not a directory: {p}", YELLOW))
        return None

    if _CODEOWNERS_TEMP_DIR:
        cached = Path(_CODEOWNERS_TEMP_DIR)
        if cached.is_dir():
            return cached

    tmp = tempfile.mkdtemp(prefix="konflux-release-data-")
    try:
        r = subprocess.run(
            ["git", "clone", "--depth", "1", "-q", KONFLUX_RELEASE_DATA_REPO, tmp],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if r.returncode != 0:
            detail = (r.stderr or r.stdout or "").strip() or "git clone failed"
            logging.warning("CODEOWNERS: could not clone konflux-release-data: %s", detail)
            shutil.rmtree(tmp, ignore_errors=True)
            return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logging.warning("CODEOWNERS: clone konflux-release-data failed: %s", e)
        shutil.rmtree(tmp, ignore_errors=True)
        return None

    _CODEOWNERS_TEMP_DIR = tmp
    if not _CODEOWNERS_ATEXIT_REGISTERED:
        atexit.register(_cleanup_codeowners_temp_dir)
        _CODEOWNERS_ATEXIT_REGISTERED = True
    print(
        color(
            f"Cloned konflux-release-data for CODEOWNERS lookups (temp: {tmp})",
            BLUE,
        )
    )
    return Path(tmp)


def _get_owners_for_namespace(codeowners_dir: Path, cluster: str, namespace: str) -> list[str]:
    """Get owner @usernames for (cluster, namespace) from CODEOWNERS. Returns list of @user."""
    if not codeowners_dir or not codeowners_dir.is_dir():
        return []
    pattern = f"/tenants-config/cluster/{cluster}/"
    first_matching_line: str | None = None
    for fname in ("CODEOWNERS", "staging/CODEOWNERS"):
        path = codeowners_dir / fname
        if not path.is_file():
            continue
        try:
            text = path.read_text()
        except OSError:
            continue
        for line in text.splitlines():
            line_stripped = line.strip()
            if not line_stripped or line_stripped.startswith("#"):
                continue
            if pattern in line_stripped and namespace in line_stripped:
                first_matching_line = line_stripped
                break
        if first_matching_line is not None:
            break
    if first_matching_line is None:
        return []
    owners = [p for p in first_matching_line.split() if p.startswith("@")]
    return sorted(set(owners))


def _get_user_display(username: str) -> str:
    """Get 'Name <email>' for a GitLab username via glab. Returns display string."""
    if not username:
        return "(unknown)"
    try:
        result = subprocess.run(
            ["glab", "api", f"users?username={username}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 or not result.stdout:
            return f"@{username} (lookup failed)"
        data = json.loads(result.stdout)
        if isinstance(data, list) and data:
            data = data[0]
        if not data:
            return f"@{username} (lookup failed)"
        name = (data.get("name") or "").strip()
        if not name:
            return f"@{username} (lookup failed)"
        email = data.get("public_email")
        if email and str(email) != "None":
            return f"{name} <{email}>"
        return f"{name} (no public email)"
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return f"@{username} (lookup failed)"


def print_per_pod_summary(
    current_run_rows: list[dict[str, str]],
    run_date_str: str,
    output_dir: Path | None = None,
    codeowners_dir: Path | None = None,
) -> None:
    """
    Print a per-pod historical summary (same format as oom_logs_and_desc_bundle_generator).
    Uses base pod names (e.g. tekton-results-api-debug) so multiple instances are collated.
    Reports only for pods found in the current run; aggregates current run + historical
    from timestamped CSVs in output_dir when provided.
    """
    # Ensure each current run row has date
    for row in current_run_rows:
        if "date" not in row:
            row["date"] = run_date_str

    all_rows = list(current_run_rows)
    if output_dir and output_dir.is_dir():
        historical = _load_historical_rows_from_output_dir(output_dir)
        all_rows = current_run_rows + historical

    if not current_run_rows:
        return

    # Base names from current run only (report only for pods found this run)
    base_names = set(_pod_base_name(row["pod"]) for row in current_run_rows)
    if not base_names:
        return

    for base_name in sorted(base_names):
        # Match rows by computed base (same base normalizes to same display name)
        matching = [r for r in all_rows if _pod_base_name(r["pod"]) == base_name]
        if not matching:
            continue

        # Aggregate by (type, date, cluster, namespace) -> count
        agg: dict[tuple[str, str, str, str], int] = defaultdict(int)
        for row in matching:
            t = (row.get("type") or "").strip() or "OOMKilled"
            if t not in ("OOMKilled", "CrashLoopBackOff"):
                continue
            key = (t, row["date"], row["cluster"], row["namespace"])
            agg[key] += 1

        print()
        print("==============================================")
        print(f"Report for pod: {base_name}")
        print("==============================================")

        for event_type in ("OOMKilled", "CrashLoopBackOff"):
            keys_for_type = [(t, d, c, ns) for (t, d, c, ns) in agg if t == event_type]
            if not keys_for_type:
                print(f"{event_type}: 0 instances (no occurrences in date-wise CSVs)")
                continue
            # Sort by date, then cluster, then namespace
            for _, date_key, cluster, namespace in sorted(
                keys_for_type, key=lambda x: (x[1], x[2], x[3])
            ):
                count = agg[(event_type, date_key, cluster, namespace)]
                if codeowners_dir:
                    owners = _get_owners_for_namespace(codeowners_dir, cluster, namespace)
                    if owners:
                        displays = [_get_user_display(u.lstrip("@")) for u in owners]
                        owner_str = ", ".join(displays)
                        owner_part = f' is owned by "{owner_str}"'
                    else:
                        owner_part = " (no owner in CODEOWNERS)"
                else:
                    owner_part = " (no CODEOWNERS repo available)"
                print(
                    f"{event_type}: {count} instance(s) on {date_key}, "
                    f"Namespace: {namespace} (cluster: {cluster}){owner_part}"
                )
        print("==============================================")


# ---------------------------
# global patterns and flags (populated in parse_args)
# ---------------------------
_INCLUDE_PATTERNS: list[Pattern] | None = None
_EXCLUDE_PATTERNS: list[Pattern] | None = None
_VERBOSE: bool = False
_LIST_NAMESPACES: bool = False


# ---------------------------
# argument parsing
# ---------------------------
def print_usage_and_exit() -> None:
    print(
        """
Usage:
  oc_get_ooms.py [OPTIONS]

Context Selection (choose one):
  --current                Run only on current-context
  --contexts ctxA,ctxB     Comma-separated context substrings (matched against available contexts)
                           If neither specified, runs on all available contexts

Parallelism & Performance:
  --batch N                Cluster-level parallelism (default: 2)
                           Maintains constant parallelism: when one cluster finishes,
                           immediately starts the next one
  --ns-batch-size M        Number of namespaces in each namespace batch (default: 10)
  --ns-workers W           Thread pool size for oc checks per namespace batch (default: 5)

Namespace Filtering:
  --include-ns regex,...   Comma-separated regex patterns to include (namespace must match any)
                           Examples: --include-ns "tenant|prod"
  --exclude-ns regex,...   Comma-separated regex patterns to exclude (if match any -> excluded)
                           Examples: --exclude-ns "test|debug"
  --include-ephemeral      Include ephemeral test and cluster namespaces (default: excluded)
                           Ephemeral namespaces include:
                           - Ephemeral cluster namespaces: clusters-<uuid> pattern
                           - Ephemeral test namespaces: test-*, e2e-*, ephemeral-*, ci-*, etc.
                           On EaaS clusters, ephemeral namespaces are excluded by default
                           to avoid false positives from temporary test environments.

Time Range Filtering:
  --time-range RANGE       Time range to look back for events (default: 1d)
                           Format: <number><unit> where unit is:
                           s=seconds, m=minutes, h=hours, d=days, M=months (30 days)
                           Examples: 30s, 1h, 6h, 1d, 7d, 1M
  --plot-range RANGE       Time range for historical graph in HTML report (default: 2M).
                           Same format as --time-range. Used with/without --print-summary-from-dir.

Resilience & Timeouts:
  --retries R              Number of retries for oc calls (default: 3)
  --timeout S              OC request timeout in seconds used as --request-timeout (default: 45)

Output:
  --output DIR             Directory to save output files (default: output)
  All output formats are generated automatically:
  - oom_results.json       Structured JSON with metadata
  - oom_results.csv        Spreadsheet-friendly CSV format
  - oom_results.table      Human-readable table format
  - oom_results.html       Standalone HTML report (open in browser)
  At the end, a per-pod summary is printed (same format as
  oom_logs_and_desc_bundle_generator). CODEOWNERS owners are resolved by shallow-cloning
  konflux-release-data to a temp directory when -c is omitted, or by using -c DIR.
  Then, for each pod in the summary, date-wise tarballs are generated (same as
  running oom_logs_and_desc_bundle_generator -p <pod> -d <output> for each pod).
  Use --no-tarballs to skip tarball generation.

  -c, --codeowners-dir DIR  Path to an existing konflux-release-data
                            checkout. If omitted, the script shallow-clones
                            releng/konflux-release-data to a temp dir
                            (requires git + network/SSH). Per-pod summary
                            shows owner (name + email via glab).

  --no-tarballs            Do not generate per-pod tarballs after the
                           run (default: generate tarballs).

Debug & Troubleshooting:
  -v, --verbose            Show which namespaces are scanned or skipped (ephemeral/include/exclude)
  --list-namespaces        Print namespaces that would be scanned
                           (per context) and exit. Use to verify a
                           namespace (e.g. preflight-dev-tenant) is
                           included.

Testing (no cluster run):
  --print-summary-from-dir [DIR]  Print per-pod summary and generate
                                   oom_results.html from existing CSVs in
                                   DIR (default: output). No cluster run.
                                   Uses oom_results.csv as \"current run\"
                                   and oom_results_*_*.csv for historical
                                   graph.

Other:
  -h, --help               Show this help message

Examples:
  # Run on current context only
  ./oc_get_ooms.py --current

  # Run on specific contexts using substrings
  ./oc_get_ooms.py --contexts kflux-prd-rh02,stone-prd-rh01

  # Custom output directory (saves CSV/JSON/HTML/TABLE and artifacts under DIR)
  ./oc_get_ooms.py --output /path/to/reports
  ./oc_get_ooms.py --current --output my-oom-run

  # High-performance mode for large clusters
  ./oc_get_ooms.py --batch 4 --ns-batch-size 250 --ns-workers 250 --timeout 200

  # Filter by time range (last 6 hours)
  ./oc_get_ooms.py --time-range 6h

  # Include only tenant namespaces, exclude test namespaces
  ./oc_get_ooms.py --include-ns tenant --exclude-ns test

  # Combine multiple options
  ./oc_get_ooms.py --contexts prod-cluster --time-range 1d --include-ns "tenant|prod" --batch 4

  # Many options together: contexts, time range, ns filters, parallelism, output dir, codeowners
  ./oc_get_ooms.py --contexts prod,staging --time-range 7d --include-ns tenant --exclude-ns test \\
    --batch 4 --ns-batch-size 50 --ns-workers 20 --retries 5 --timeout 120 \\
    --output my-reports -c /path/to/konflux-release-data --verbose

  # All contexts, last 7 days, with custom parallelism
  ./oc_get_ooms.py --time-range 7d --batch 8 --ns-batch-size 100 --ns-workers 50

  # Regenerate HTML report from existing CSVs (no cluster run)
  ./oc_get_ooms.py --print-summary-from-dir output
  ./oc_get_ooms.py --print-summary-from-dir /path/to/output

  # Verify which namespaces will be scanned (e.g. check if preflight-dev-tenant is included)
  ./oc_get_ooms.py --contexts stone-stg-rh01 --list-namespaces | grep preflight

  # Verbose run to see skipped vs scanned namespaces
  ./oc_get_ooms.py --contexts stone-stg-rh01 --verbose --time-range 1d
"""
    )
    sys.exit(1)


def compile_patterns(csv_patterns: str | None) -> list[Pattern] | None:
    if not csv_patterns:
        return None
    parts = [p.strip() for p in csv_patterns.split(",") if p.strip()]
    if not parts:
        return None
    try:
        return [re.compile(p) for p in parts]
    except re.error as e:
        print(color(f"Invalid regex in patterns: {e}", RED))
        sys.exit(1)


def parse_args(
    argv: list[str],
) -> tuple[
    list[str],
    int,
    int,
    int,
    int,
    int | None,
    str,
    int,
    str,
    bool,
    bool,
    bool,
    str | None,
    str | None,
    str,
]:
    args = list(argv)
    if "--help" in args or "-h" in args:
        print_usage_and_exit()

    contexts: list[str] = []
    batch_size = DEFAULT_BATCH_SIZE
    ns_batch_size = DEFAULT_NS_BATCH_SIZE
    ns_workers = DEFAULT_NS_WORKERS
    retries = DEFAULT_RETRIES
    oc_timeout_seconds = DEFAULT_OC_TIMEOUT
    include_csv = None
    exclude_csv = None
    time_range_str = "1d"  # Default 1 day
    plot_range_str = "2M"  # Default 2 months for historical graph
    exclude_ephemeral = True  # Default: exclude ephemeral namespaces
    verbose = False
    list_namespaces = False
    codeowners_dir: str | None = None
    print_summary_from_dir: str | None = None
    output_dir_str = "output"
    no_tarballs = "--no-tarballs" in args

    if "--output" in args:
        i = args.index("--output")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --output", RED))
            print_usage_and_exit()
        output_dir_str = args[i + 1]

    if "--print-summary-from-dir" in args:
        i = args.index("--print-summary-from-dir")
        if i + 1 < len(args) and not args[i + 1].startswith("-"):
            print_summary_from_dir = args[i + 1].strip()
        else:
            print_summary_from_dir = output_dir_str

    if "--current" in args:
        cur = get_current_context(retries=retries, oc_timeout_seconds=oc_timeout_seconds)
        if cur:
            contexts = [cur]
    elif "--contexts" in args:
        i = args.index("--contexts")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --contexts", RED))
            print_usage_and_exit()
        context_substrings = [c.strip() for c in args[i + 1].split(",") if c.strip()]
        # Get all available contexts and match substrings
        available_contexts = get_all_contexts(
            retries=retries, oc_timeout_seconds=oc_timeout_seconds
        )
        if not available_contexts:
            print(
                color(
                    "ERROR: Could not retrieve available contexts. "
                    "Please check your oc/kubectl configuration.",
                    RED,
                )
            )
            sys.exit(1)
        contexts = match_contexts_by_substring(context_substrings, available_contexts)
    else:
        contexts = get_all_contexts(retries=retries, oc_timeout_seconds=oc_timeout_seconds)

    if "--batch" in args:
        i = args.index("--batch")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --batch", RED))
            print_usage_and_exit()
        try:
            batch_size = int(args[i + 1])
            if batch_size < 1:
                raise ValueError("batch size must be >= 1")
        except (ValueError, IndexError) as e:
            print(color(f"ERROR: invalid --batch value: {e}", RED))
            print_usage_and_exit()

    if "--ns-batch-size" in args:
        i = args.index("--ns-batch-size")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --ns-batch-size", RED))
            print_usage_and_exit()
        try:
            ns_batch_size = int(args[i + 1])
            if ns_batch_size < 1:
                raise ValueError("ns-batch-size must be >= 1")
        except (ValueError, IndexError) as e:
            print(color(f"ERROR: invalid --ns-batch-size value: {e}", RED))
            print_usage_and_exit()

    if "--ns-workers" in args:
        i = args.index("--ns-workers")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --ns-workers", RED))
            print_usage_and_exit()
        try:
            ns_workers = int(args[i + 1])
            if ns_workers < 1:
                raise ValueError("ns-workers must be >= 1")
        except (ValueError, IndexError) as e:
            print(color(f"ERROR: invalid --ns-workers value: {e}", RED))
            print_usage_and_exit()

    if "--include-ns" in args:
        i = args.index("--include-ns")
        include_csv = args[i + 1] if i + 1 < len(args) else None

    if "--exclude-ns" in args:
        i = args.index("--exclude-ns")
        exclude_csv = args[i + 1] if i + 1 < len(args) else None

    if "--include-ephemeral" in args:
        exclude_ephemeral = False  # User wants to include ephemeral namespaces

    if "--verbose" in args or "-v" in args:
        verbose = True

    if "--list-namespaces" in args:
        list_namespaces = True

    if "--retries" in args:
        i = args.index("--retries")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --retries", RED))
            print_usage_and_exit()
        try:
            retries = int(args[i + 1])
            if retries < 1:
                raise ValueError("retries must be >= 1")
        except (ValueError, IndexError) as e:
            print(color(f"ERROR: invalid --retries value: {e}", RED))
            print_usage_and_exit()

    if "--timeout" in args:
        i = args.index("--timeout")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --timeout", RED))
            print_usage_and_exit()
        try:
            oc_timeout_seconds = int(args[i + 1])
            if oc_timeout_seconds < 1:
                raise ValueError("timeout must be >= 1")
        except (ValueError, IndexError) as e:
            print(color(f"ERROR: invalid --timeout value: {e}", RED))
            print_usage_and_exit()

    if "--time-range" in args:
        i = args.index("--time-range")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --time-range", RED))
            print_usage_and_exit()
        time_range_str = args[i + 1]
        try:
            # Validate the format
            parse_time_range(time_range_str)
        except ValueError as e:
            print(color(f"ERROR: invalid --time-range value: {e}", RED))
            print_usage_and_exit()

    if "--plot-range" in args:
        i = args.index("--plot-range")
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --plot-range", RED))
            print_usage_and_exit()
        plot_range_str = args[i + 1]
        try:
            parse_time_range(plot_range_str)
        except ValueError as e:
            print(color(f"ERROR: invalid --plot-range value: {e}", RED))
            print_usage_and_exit()

    if "--codeowners-dir" in args or "-c" in args:
        flag = "--codeowners-dir" if "--codeowners-dir" in args else "-c"
        i = args.index(flag)
        if i + 1 >= len(args):
            print(color("ERROR: missing argument for --codeowners-dir", RED))
            print_usage_and_exit()
        codeowners_dir = args[i + 1].strip()
        if not codeowners_dir:
            codeowners_dir = None

    global _INCLUDE_PATTERNS, _EXCLUDE_PATTERNS, _VERBOSE, _LIST_NAMESPACES
    _INCLUDE_PATTERNS = compile_patterns(include_csv)
    _EXCLUDE_PATTERNS = compile_patterns(exclude_csv)
    _VERBOSE = verbose
    _LIST_NAMESPACES = list_namespaces

    # Parse time range to seconds
    try:
        time_range_seconds = parse_time_range(time_range_str)
    except ValueError:
        time_range_seconds = 86400  # Default to 1 day if parsing fails
    try:
        plot_range_seconds = parse_time_range(plot_range_str)
    except ValueError:
        plot_range_seconds = 5184000  # 2 months in seconds

    return (
        contexts,
        batch_size,
        ns_batch_size,
        ns_workers,
        retries,
        oc_timeout_seconds,
        time_range_seconds,
        time_range_str,
        plot_range_seconds,
        plot_range_str,
        exclude_ephemeral,
        verbose,
        list_namespaces,
        codeowners_dir,
        print_summary_from_dir,
        output_dir_str,
        no_tarballs,
    )


# ---------------------------
# main
# ---------------------------
def main() -> None:
    """Main entry point for the OOM/CrashLoopBackOff detector."""
    # Configure logging (quiet by default, can be enhanced with --verbose flag)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    (
        contexts,
        batch_size,
        ns_batch_size,
        ns_workers,
        retries,
        oc_timeout_seconds,
        time_range_seconds,
        time_range_str,
        plot_range_seconds,
        plot_range_str,
        exclude_ephemeral,
        verbose,
        list_namespaces,
        codeowners_dir,
        print_summary_from_dir,
        output_dir_str,
        no_tarballs,
    ) = parse_args(sys.argv[1:])

    # --print-summary-from-dir: print summary and generate HTML from existing CSVs (no cluster run)
    if print_summary_from_dir is not None:
        # Resolve relative paths (e.g. "output") relative to the script's directory,
        # so the same dir is used regardless of current working directory.
        p = Path(print_summary_from_dir)
        if not p.is_absolute():
            script_dir = Path(__file__).resolve().parent
            out_dir = (script_dir / print_summary_from_dir).resolve()
        else:
            out_dir = p.resolve()
        main_csv = out_dir / "oom_results.csv"
        if not main_csv.is_file():
            print(
                color(
                    f"ERROR: {main_csv} not found. Run oc_get_ooms.py first to generate CSVs.", RED
                )
            )
            sys.exit(1)
        mtime = main_csv.stat().st_mtime
        run_date_str = datetime.fromtimestamp(mtime).strftime("%d-%b-%Y")
        current_run_rows = _read_csv_rows_with_date(main_csv, run_date_str)
        codeowners_path = resolve_codeowners_dir(codeowners_dir)
        print_per_pod_summary(
            current_run_rows,
            run_date_str,
            output_dir=out_dir,
            codeowners_dir=codeowners_path,
        )
        # Generate oom_results.html from existing data (graph + summary + detailed findings)
        if generate_html_report is not None:
            historical_series = build_historical_series_from_output_dir(out_dir, plot_range_seconds)
            historical_series_by_cluster = build_historical_series_by_cluster_from_output_dir(
                out_dir, plot_range_seconds
            )
            historical_html_links = get_historical_html_links(out_dir)
            if historical_series:
                print(
                    color(f"Historical graph: {len(historical_series)} run(s) in plot range.", BLUE)
                )
            else:
                print(
                    color(
                        "Historical graph: no timestamped runs in plot range"
                        " (oom_results_*_*.csv).",
                        YELLOW,
                    )
                )
            html_path = out_dir / "oom_results.html"
            try:
                generate_html_report(
                    rows=current_run_rows,
                    time_range_str=time_range_str,
                    html_path=html_path,
                    report_generated_est=report_generated_est(),
                    historical_series=historical_series,
                    historical_series_by_cluster=historical_series_by_cluster,
                    historical_html_links=historical_html_links,
                    plot_range_str=plot_range_str,
                )
                print(color(f"HTML report written → {html_path}", GREEN))
            except Exception as e:
                logging.warning(f"Failed to write HTML report: {e}")
                print(color(f"WARNING: Failed to write HTML report: {e}", YELLOW))
        sys.exit(0)

    if not contexts:
        print(color("No contexts discovered. Exiting.", RED))
        sys.exit(1)

    # --list-namespaces: print namespaces that would be scanned per context and exit
    if list_namespaces:
        for ctx in contexts:
            namespaces = get_namespaces_for_context(
                ctx,
                retries=retries,
                oc_timeout_seconds=oc_timeout_seconds,
                include_patterns=_INCLUDE_PATTERNS,
                exclude_patterns=_EXCLUDE_PATTERNS,
                exclude_ephemeral=exclude_ephemeral,
            )
            cluster = short_cluster_name(ctx)
            print(
                color(f"Context: {ctx} (cluster: {cluster}) — {len(namespaces)} namespaces", BLUE)
            )
            for ns in sorted(namespaces):
                print(ns)
        sys.exit(0)

    print(color(f"Using contexts: {contexts}", BLUE))
    print(
        color(
            f"Cluster-parallelism: {batch_size}  NS-batch-size: {ns_batch_size}  "
            f"NS-workers: {ns_workers}",
            BLUE,
        )
    )
    print(
        color(
            f"Retries: {retries}  OC timeout(s): {oc_timeout_seconds}s  "
            f"Time-range: {time_range_str}",
            BLUE,
        )
    )
    if exclude_ephemeral:
        print(
            color(
                "Ephemeral namespaces: EXCLUDED"
                " (ephemeral test/cluster namespaces will be skipped)",
                BLUE,
            )
        )
    else:
        print(
            color(
                "Ephemeral namespaces: INCLUDED (all namespaces will be scanned)",
                YELLOW,
            )
        )
    if _INCLUDE_PATTERNS:
        print(
            color(
                f"Include namespace patterns: {[p.pattern for p in _INCLUDE_PATTERNS]}",
                BLUE,
            )
        )
    if _EXCLUDE_PATTERNS:
        print(
            color(
                f"Exclude namespace patterns: {[p.pattern for p in _EXCLUDE_PATTERNS]}",
                BLUE,
            )
        )

    # Check cluster connectivity; proceed only if at least one cluster is connected
    _all_connected, connectivity_report = check_all_clusters_connectivity(
        contexts, retries=retries, oc_timeout_seconds=oc_timeout_seconds
    )
    print_connectivity_report_summary(connectivity_report)

    at_least_one_connected = any(connected for _, connected, _ in connectivity_report)
    if not at_least_one_connected:
        print(color("No clusters are accessible. Aborting.", RED))
        sys.exit(1)

    # Ensure output directory exists
    output_dir = ensure_output_directory(output_dir_str)

    # Move existing output files to output directory (one-time migration)
    move_existing_output_files(output_dir)

    results, skipped = run_batches(
        contexts,
        batch_size,
        retries,
        oc_timeout_seconds,
        ns_batch_size,
        ns_workers,
        time_range_seconds,
        exclude_ephemeral,
        output_dir=output_dir,
    )

    # All output files go to 'output' subdirectory
    json_path = output_dir / "oom_results.json"
    csv_path = output_dir / "oom_results.csv"
    table_path = output_dir / "oom_results.table"
    html_path = output_dir / "oom_results.html"

    # Backup existing files before generating new ones
    backup_output_files(json_path, csv_path, table_path, html_path)

    export_results(
        results,
        json_path,
        csv_path,
        table_path,
        html_path,
        time_range_str,
        output_dir=output_dir,
        plot_range_seconds=plot_range_seconds,
        plot_range_str=plot_range_str,
    )

    pretty_print(results, skipped)

    if skipped:
        print(
            color(
                "\nSome clusters were skipped due to connectivity errors (see messages above).",
                YELLOW,
            )
        )

    print(
        color(
            "\nPer-cluster logs written to"
            " output/logs_and_description_files/<cluster>/"
            " (if any findings were found)",
            GREEN,
        )
    )
    print(
        color(
            f"Output files written to '{output_dir}/' directory",
            GREEN,
        )
    )

    # Per-pod summary (base names, current run + historical from output dir)
    run_date_str = datetime.now().strftime("%d-%b-%Y")
    current_run_rows = collect_rows(results, "")
    for row in current_run_rows:
        row["date"] = run_date_str
    codeowners_path = resolve_codeowners_dir(codeowners_dir)
    print_per_pod_summary(
        current_run_rows,
        run_date_str,
        output_dir=output_dir,
        codeowners_dir=codeowners_path,
    )

    # Generate per-pod tarballs (same as oom_logs_and_desc_bundle_generator
    # -p <pod> -d <output> for each pod).
    # The bundle generator matches CSV pod column by substring; we must
    # pass a literal that appears in
    # actual pod names (not the display base name like "apiserver-*" which has asterisks).
    if not no_tarballs and current_run_rows:
        script_dir = Path(__file__).resolve().parent
        bundle_gen = script_dir / "oom_logs_and_desc_bundle_generator"
        # Group full pod names by display base_name, then compute a
        # match string (longest common prefix)
        base_to_pods: dict[str, list[str]] = defaultdict(list)
        for row in current_run_rows:
            base_to_pods[_pod_base_name(row["pod"])].append(row["pod"])
        if bundle_gen.is_file():
            print()
            print(color(f"Generating tarballs for {len(base_to_pods)} pod(s) ...", BLUE))
            for base_name in sorted(base_to_pods.keys()):
                pod_names = base_to_pods[base_name]
                match_str = _match_string_for_bundle_generator(pod_names)
                if not match_str:
                    continue
                cmd = [
                    "bash",
                    str(bundle_gen),
                    "-p",
                    match_str,
                    "-d",
                    str(output_dir),
                ]
                if codeowners_path is not None and codeowners_path.is_dir():
                    cmd.extend(["-c", str(codeowners_path)])
                rc = subprocess.run(cmd, cwd=str(script_dir))
                if rc.returncode != 0:
                    print(
                        color(
                            f"  Warning: tarball generation for pod"
                            f" '{base_name}' exited with {rc.returncode}",
                            YELLOW,
                        )
                    )
        else:
            print(color(f"  Skipping tarballs: {bundle_gen} not found", YELLOW))


if __name__ == "__main__":
    main()
