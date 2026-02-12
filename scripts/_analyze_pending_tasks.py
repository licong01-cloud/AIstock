"""Analyze pending tasks in aistock_task_catalog."""
import json
from urllib.request import urlopen

# Check all pending tasks
resp = urlopen("http://127.0.0.1:8001/api/v1/rdagent/tasks/local?limit=100", timeout=30)
data = json.loads(resp.read().decode("utf-8"))
tasks = data.get("items", [])

pending = [t for t in tasks if t.get("sync_status") == "pending"]
print(f"Total tasks: {len(tasks)}")
print(f"Pending tasks: {len(pending)}")

print("\n=== Pending tasks with a]a] prefix ===")
for t in pending:
    if "a]a]" in t.get("task_id", ""):
        print(f"  {t.get('task_id')} - updated: {t.get('updated_at_utc')}")

print("\n=== All pending tasks (first 10) ===")
for t in pending[:10]:
    print(f"  {t.get('task_id')}")
    print(f"    updated: {t.get('updated_at_utc')}")
    print(f"    task_dir: {t.get('task_dir')}")
    print(f"    manifest_path: {t.get('manifest_path')}")
