"""Check all pending tasks and their creation time pattern."""
import sys
sys.path.insert(0, r"f:\Dev\AIstock\backend")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(r"f:\Dev\AIstock\.env"))

from db.pg_pool import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        # Get all pending tasks with full details
        print("=== All pending tasks ===")
        cur.execute("""
            SELECT task_id, sync_status, task_dir, manifest_path, updated_at_utc, log_dir
            FROM aistock_task_catalog
            WHERE sync_status = 'pending'
            ORDER BY updated_at_utc DESC
        """)
        for row in cur.fetchall():
            print(f"  task_id: {row[0]}")
            print(f"  sync_status: {row[1]}")
            print(f"  task_dir: {row[2]}")
            print(f"  manifest_path: {row[3]}")
            print(f"  updated_at_utc: {row[4]}")
            print(f"  log_dir: {row[5]}")
            print()

        # Check if there's any pattern in the timestamps
        print("\n=== Timestamp analysis ===")
        cur.execute("""
            SELECT DATE_TRUNC('minute', updated_at_utc) as minute, COUNT(*)
            FROM aistock_task_catalog
            WHERE sync_status = 'pending'
            GROUP BY DATE_TRUNC('minute', updated_at_utc)
            ORDER BY minute DESC
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} tasks")
