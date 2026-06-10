import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

token = sys.argv[1]
host = sys.argv[2]
task_name = sys.argv[3]
end_time_in_secs = int(sys.argv[4])
last_num_days = int(sys.argv[5])

url = f"https://{host}/api/v1/query_range"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}
params = {
    "query": (f'kube_pod_labels{{label_tekton_dev_task="{task_name}",namespace=~".*-tenant"}}'),
    "step": 15 * last_num_days,
    "start": end_time_in_secs - (last_num_days * 24 * 60 * 60),
    "end": end_time_in_secs,
}

response = requests.get(url, headers=headers, params=params, verify=False, timeout=60)  # nosec B501
print(response.text)
