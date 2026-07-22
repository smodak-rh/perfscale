import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

token = sys.argv[1]
host = sys.argv[2]
task_name = sys.argv[3]
end_time_in_secs = int(sys.argv[4])
# argv[5]: lookback window in seconds (supports sub-day windows)
lookback_seconds = int(sys.argv[5])
if lookback_seconds <= 0:
    lookback_seconds = 86400

url = f"https://{host}/api/v1/query_range"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}
# Keep similar sample density to the old "15 * days" step.
step = max(15, lookback_seconds // 5760)
params = {
    "query": (f'kube_pod_labels{{label_tekton_dev_task="{task_name}",namespace=~".*-tenant"}}'),
    "step": step,
    "start": end_time_in_secs - lookback_seconds,
    "end": end_time_in_secs,
}

response = requests.get(url, headers=headers, params=params, verify=False, timeout=60)  # nosec B501
print(response.text)
