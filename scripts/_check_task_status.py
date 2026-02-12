"""Check task sync status."""
import json
from urllib.request import urlopen

resp = urlopen("http://127.0.0.1:8001/api/v1/rdagent/tasks/local?limit=200", timeout=30)
data = json.loads(resp.read().decode("utf-8"))
tasks = data.get("tasks", [])

pending = [t for t in tasks if t.get("sync_status") == "pending"]
synced = [t for t in tasks if t.get("sync_status") == "success"]
failed = [t for t in tasks if t.get("sync_status") == "failed"]
other = [t for t in tasks if t.get("sync_status") not in ("pending", "success", "failed")]

print(f"Total: {len(tasks)}")
print(f"Pending: {len(pending)}")
print(f"Synced: {len(synced)}")
print(f"Failed: {len(failed)}")
print(f"Other: {len(other)}")

if synced:
    print(f"\nSynced tasks (first 5):")
    for t in synced[:5]:
        print(f"  {t.get('task_run_id')} - {t.get('sync_status')}")

if pending:
    print(f"\nPending tasks (first 5):")
    for t in pending[:5]:
        print(f"  {t.get('task_run_id')} - {t.get('sync_status')}")
