"""Deep analysis of a]a] tasks - check all tables."""
import json
from urllib.request import urlopen

# Check rdagent.rdagent_candidate_tasks table via sync-candidates API
print("=== Checking sync-candidates API ===")
try:
    resp = urlopen("http://127.0.0.1:8001/api/v1/rdagent/tasks/sync-candidates?limit=100", timeout=60)
    data = json.loads(resp.read().decode("utf-8"))
    items = data.get("items", [])
    aa_tasks = [t for t in items if "a]a]" in str(t.get("task_id", ""))]
    print(f"Total candidates: {len(items)}")
    print(f"a]a] tasks in candidates: {len(aa_tasks)}")
    for t in aa_tasks:
        print(f"  {t}")
except Exception as e:
    print(f"Error: {e}")

# Check local tasks
print("\n=== Checking local tasks API ===")
try:
    resp = urlopen("http://127.0.0.1:8001/api/v1/rdagent/tasks/local?limit=100", timeout=30)
    data = json.loads(resp.read().decode("utf-8"))
    items = data.get("items", [])
    aa_tasks = [t for t in items if "a]a]" in str(t.get("task_id", ""))]
    print(f"Total local tasks: {len(items)}")
    print(f"a]a] tasks in local: {len(aa_tasks)}")
    for t in aa_tasks:
        print(f"  task_id: {t.get('task_id')}")
        print(f"  updated_at_utc: {t.get('updated_at_utc')}")
        print(f"  sync_status: {t.get('sync_status')}")
        print(f"  task_dir: {t.get('task_dir')}")
        print()
except Exception as e:
    print(f"Error: {e}")
