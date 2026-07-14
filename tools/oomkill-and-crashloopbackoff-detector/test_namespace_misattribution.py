#!/usr/bin/env python3
"""
Test cases for namespace misattribution fix (KONFLUX-14702).

Verifies that namespace_worker_oc() correctly:
1. Filters events by involvedObject.kind == "Pod"
2. Drops event-only pods that don't exist in the namespace's pod listing
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent))

from oc_get_ooms import namespace_worker_oc


def make_event(
    reason: str,
    pod_name: str,
    kind: str = "Pod",
    timestamp: str = "2026-06-20T12:00:00Z",
):
    return {
        "reason": reason,
        "involvedObject": {
            "kind": kind,
            "name": pod_name,
            "namespace": "test-ns",
        },
        "eventTime": timestamp,
        "lastTimestamp": timestamp,
    }


def make_pod_item(name: str, labels=None):
    return {
        "metadata": {"name": name, "labels": labels or {}},
        "status": {
            "containerStatuses": [
                {
                    "name": "main",
                    "state": {"running": {}},
                    "lastState": {},
                    "restartCount": 0,
                }
            ]
        },
    }


@patch("oc_get_ooms.get_all_events_oc")
@patch("oc_get_ooms.get_pods_items")
@patch("oc_get_ooms.oomkilled_via_pods_oc", return_value=[])
@patch("oc_get_ooms.crashloop_via_pods_oc", return_value=[])
def test_non_pod_events_are_ignored(mock_crash, mock_oom, mock_pods, mock_events):
    """Events with involvedObject.kind != 'Pod' must be ignored."""
    mock_events.return_value = [
        make_event("BackOff", "splunkforwarder-ds-abc12", kind="DaemonSet"),
        make_event("OOMKilling", "some-node", kind="Node"),
        make_event("BackOff", "managed-upgrade-operator-xyz", kind="Pod"),
    ]
    mock_pods.return_value = [make_pod_item("managed-upgrade-operator-xyz")]

    result = namespace_worker_oc(
        "ctx",
        "openshift-managed-upgrade-operator",
        retries=1,
        oc_timeout_seconds=10,
    )

    assert result is not None, "Should find the Pod event"
    assert "managed-upgrade-operator-xyz" in result, "Pod event should be included"
    assert "splunkforwarder-ds-abc12" not in result, "DaemonSet event must be excluded"
    assert "some-node" not in result, "Node event must be excluded"
    print("PASS: Non-Pod events are correctly ignored")


@patch("oc_get_ooms.get_all_events_oc")
@patch("oc_get_ooms.get_pods_items")
@patch("oc_get_ooms.oomkilled_via_pods_oc", return_value=[])
@patch("oc_get_ooms.crashloop_via_pods_oc", return_value=[])
def test_event_only_pods_not_in_listing_are_dropped(mock_crash, mock_oom, mock_pods, mock_events):
    """Pods found only via events but missing from pod listing must be dropped."""
    mock_events.return_value = [
        make_event("BackOff", "ghost-pod-from-stale-event", kind="Pod"),
        make_event("BackOff", "real-pod-abc12", kind="Pod"),
    ]
    mock_pods.return_value = [make_pod_item("real-pod-abc12")]

    result = namespace_worker_oc("ctx", "test-ns", retries=1, oc_timeout_seconds=10)

    assert result is not None
    assert "real-pod-abc12" in result, "Pod that exists in listing should be kept"
    assert "ghost-pod-from-stale-event" not in result, (
        "Event-only pod not in listing must be dropped"
    )
    print("PASS: Event-only pods not in listing are correctly dropped")


@patch("oc_get_ooms.get_all_events_oc")
@patch("oc_get_ooms.get_pods_items")
@patch("oc_get_ooms.oomkilled_via_pods_oc")
@patch("oc_get_ooms.crashloop_via_pods_oc", return_value=[])
def test_pod_status_detected_pods_are_kept(mock_crash, mock_oom, mock_pods, mock_events):
    """Pods found via pod status (oc_get_pods) should always be kept."""
    mock_events.return_value = []
    mock_pods.return_value = [make_pod_item("crash-pod-xyz")]
    mock_oom.return_value = [
        {
            "pod": "crash-pod-xyz",
            "timestamp": "2026-06-20T12:00:00Z",
            "application": "",
            "component": "",
        }
    ]

    result = namespace_worker_oc("ctx", "test-ns", retries=1, oc_timeout_seconds=10)

    assert result is not None
    assert "crash-pod-xyz" in result, "Pod detected via pod status should be kept"
    assert result["crash-pod-xyz"]["oom_timestamps"], "Should have OOM timestamps"
    print("PASS: Pod-status-detected pods are correctly kept")


@patch("oc_get_ooms.get_all_events_oc")
@patch("oc_get_ooms.get_pods_items")
@patch("oc_get_ooms.oomkilled_via_pods_oc", return_value=[])
@patch("oc_get_ooms.crashloop_via_pods_oc", return_value=[])
def test_no_false_negatives_for_real_event_pods(mock_crash, mock_oom, mock_pods, mock_events):
    """Pods found via events AND present in pod listing should be kept."""
    mock_events.return_value = [
        make_event("BackOff", "real-crashing-pod-abc12", kind="Pod"),
    ]
    mock_pods.return_value = [make_pod_item("real-crashing-pod-abc12")]

    result = namespace_worker_oc("ctx", "test-ns", retries=1, oc_timeout_seconds=10)

    assert result is not None
    assert "real-crashing-pod-abc12" in result, "Event pod present in listing should be kept"
    print("PASS: Real event pods in listing are correctly kept")


if __name__ == "__main__":
    test_non_pod_events_are_ignored()
    test_event_only_pods_not_in_listing_are_dropped()
    test_pod_status_detected_pods_are_kept()
    test_no_false_negatives_for_real_event_pods()
    print("\nAll namespace misattribution tests passed!")
