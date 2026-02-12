"""Search for all _upsert_task_catalog calls and their sync_status values."""
import re

with open(r"f:\Dev\AIstock\backend\services\rdagent_task_sync_service.py", "r", encoding="utf-8") as f:
    content = f.read()

# Find all _upsert_task_catalog calls
pattern = r'_upsert_task_catalog\s*\(\s*\n?\s*(\w+),\s*\n?\s*\{([^}]+)\}'
matches = re.findall(pattern, content, re.DOTALL)

print(f"Found {len(matches)} _upsert_task_catalog calls:\n")
for i, (task_var, data_block) in enumerate(matches, 1):
    # Extract sync_status value
    sync_match = re.search(r'"sync_status":\s*"(\w+)"', data_block)
    sync_status = sync_match.group(1) if sync_match else "NOT_FOUND"
    print(f"{i}. task_var={task_var}, sync_status={sync_status}")
    print(f"   Data block preview: {data_block[:100].strip()}...")
    print()
