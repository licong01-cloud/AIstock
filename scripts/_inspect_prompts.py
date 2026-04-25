"""Inspect qe_prompts status — check which agent prompts exist and their versions."""
import os
from pathlib import Path

for line in (Path(r"F:/Dev/AIstock/.env")).read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line: continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import psycopg2
conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ["TDX_DB_PORT"]),
    dbname=os.environ["TDX_DB_NAME"], user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
)

# Find prompts table
with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND (table_name LIKE '%prompt%')
        ORDER BY table_name
    """)
    print("=== prompt tables ===")
    for (t,) in cur.fetchall(): print(f"  {t}")

# Get columns
with conn.cursor() as cur:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='qe_agent_prompts'
        ORDER BY ordinal_position
    """)
    print("\n=== qe_prompts columns ===")
    for name, dt in cur.fetchall():
        print(f"  {name:30s} {dt}")

# All prompts
with conn.cursor() as cur:
    cur.execute("""
        SELECT agent_type, prompt_key, version, is_active,
               LENGTH(system_prompt) AS sp_len,
               updated_at
        FROM qe_agent_prompts
    """)
    print("\n=== all prompts ===")
    for row in cur.fetchall():
        print(f"  agent={row[0]:25s} key={row[1]:30s} v{row[2]}  active={row[3]}  sp_len={row[4]}  upd={row[5]}")

# Sample active factor_analyst/analyze_factor_v2 content
with conn.cursor() as cur:
    cur.execute("""
        SELECT system_prompt, user_prompt_template FROM qe_agent_prompts
        WHERE agent_type='factor_analyst' AND prompt_key='analyze_factor_v2' AND is_active=TRUE
        ORDER BY version DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        print("\n=== factor_analyst/analyze_factor_v2 (active) — system_prompt first 3500 chars ===")
        print(row[0][:3500])
        print("\n=== user_prompt_template (first 1000 chars) ===")
        print((row[1] or "(none)")[:1000])
    else:
        print("\n[WARNING] no active factor_analyst/analyze_factor_v2 prompt found")

conn.close()
