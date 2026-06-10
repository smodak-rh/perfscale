#!/usr/bin/env python3
import os
import sys

import requests
import urllib3

urllib3.disable_warnings()

if len(sys.argv) != 2:
    print("Usage: query_prometheus_instant.py '<promql>'", file=sys.stderr)
    sys.exit(1)

query = sys.argv[1]
token = os.environ.get("PROM_TOKEN")
host = os.environ.get("PROM_HOST")

if not token or not host:
    print("PROM_TOKEN and PROM_HOST must be set", file=sys.stderr)
    sys.exit(1)

resp = requests.get(
    f"https://{host}/api/v1/query",
    headers={"Authorization": f"Bearer {token}"},
    params={"query": query},
    verify=False,  # nosec B501
    timeout=180,
)
resp.raise_for_status()
print(resp.text)
