import json
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if len(sys.argv) <= 6:
    sys.exit(1)

token = sys.argv[1]
host = sys.argv[2]
step_name = sys.argv[3]
end_time_in_secs = int(sys.argv[4])
pod_name = sys.argv[5]
last_num_days = int(sys.argv[6])

url = f"https://{host}/api/v1/query_range"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}
params = {
    "query": (
        "container_memory_max_usage_bytes"
        f'{{namespace=~".*-tenant",container="{step_name}",'
        f'pod=~"({pod_name})"}}'
    ),
    "step": 15 * last_num_days,
    "start": end_time_in_secs - (last_num_days * 24 * 60 * 60),
    "end": end_time_in_secs,
}

response = requests.get(url, headers=headers, params=params, verify=False, timeout=60)  # nosec B501

if response.status_code == 200:
    print(json.dumps(response.json(), indent=4))
else:
    print(f"Request failed with status code: {response.status_code}")
