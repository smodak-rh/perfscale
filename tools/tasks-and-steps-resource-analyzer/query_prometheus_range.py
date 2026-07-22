#!/usr/bin/env python3
import sys
import time
import traceback

import requests
import urllib3

urllib3.disable_warnings()

if len(sys.argv) != 6:
    print(
        f"Usage: {sys.argv[0]} <token> <host> <query> <start> <end>",
        file=sys.stderr,
    )
    sys.exit(2)

token, host, query, start, end = sys.argv[1:]

url = f"https://{host}/api/v1/query_range"

headers = {
    "Authorization": f"Bearer {token}",
}

# Calculate time range in seconds
start_ts = int(start)
end_ts = int(end)
duration = end_ts - start_ts

# Adaptive step size based on time range to avoid Prometheus limits
# Prometheus typically limits to ~11,000 data points per query
# For longer ranges, use larger steps to stay within limits
if duration <= 86400:  # <= 1 day
    step = "30s"
elif duration <= 604800:  # <= 7 days
    step = "5m"  # 5 minutes for 7 days = ~2016 data points
elif duration <= 2592000:  # <= 30 days
    step = "15m"  # 15 minutes for 30 days = ~2880 data points
else:
    step = "1h"  # 1 hour for longer ranges

params = {
    "query": query,
    "start": start,
    "end": end,
    "step": step,
}

try:
    t0 = time.time()
    resp = requests.get(
        url,
        headers=headers,
        params=params,
        verify=False,
        timeout=900,  # nosec B501
    )  # 15 minutes timeout
    if resp.status_code != 200:
        body = (resp.text or "")[:500].replace("\n", " ")
        print(
            f"HTTP {resp.status_code} from {host} ({time.time() - t0:.1f}s): {body}",
            file=sys.stderr,
        )
        sys.exit(1)
    print(resp.text)
except Exception as exc:
    print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
