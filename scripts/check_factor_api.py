"""Check factor expression and code_text via API."""
import json
from urllib.request import urlopen

resp = urlopen("http://127.0.0.1:8001/api/v1/rdagent/catalogs/factors?limit=2&offset=0", timeout=10)
data = json.loads(resp.read().decode("utf-8"))
for item in data.get("items", []):
    print(f"\n=== {item['name']} ===")
    print(f"expression: {repr(item.get('expression'))}")
    print(f"performance_metrics: {item.get('performance_metrics')}")
    print(f"source_code_relpath: {item.get('source_code_relpath')}")
    print(f"source_task_id: {item.get('source_task_id')}")
