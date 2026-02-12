"""Check actual DB table definition for aistock_task_catalog."""
import json
from urllib.request import urlopen, Request

# Use a simple SQL query via API if available, or check schema
# Let's check if there's a health/debug endpoint

# First, let's see what the actual column default is
# We need to query information_schema

# Since we can't directly query DB, let's analyze the code flow
# The pending tasks have:
# - sync_status = "pending"
# - task_dir = None
# - manifest_path = None
# - updated_at_utc around 2026-02-07 to 2026-02-10

# This suggests they were written by some batch process around that time
# Let's check if there's a sync-all or batch import endpoint

print("Analyzing pending tasks pattern...")
print("All pending tasks have:")
print("  - sync_status = 'pending'")
print("  - task_dir = None")
print("  - manifest_path = None")
print("")
print("This indicates they were inserted WITHOUT going through sync_task_from_log()")
print("which always sets sync_status to 'syncing' first, then 'success' or 'failed'")
print("")
print("The 'pending' value must come from:")
print("1. A DEFAULT value on the sync_status column in the actual DB")
print("2. Or some other code path that inserts with sync_status='pending'")
