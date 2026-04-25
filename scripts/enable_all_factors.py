"""Phase 0: Enable all factors disabled under v1 rules.

Policy:
- Enable any factor with (is_available=FALSE AND disable_reason IS NULL)
  → these are v1-era disables with no audit trail, must re-evaluate under v2
- Preserve factors with explicit disable_reason in:
    {'data_source_deprecated', 'legal', 'manual_override'}
  → these are real business-level decisions, do not touch
- Reset rehab_candidate/last_rehab_at (they were placeholders)

Output: before/after counts + list of re-enabled factor names (first 30).
"""
import os, sys
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import psycopg2

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ["TDX_DB_PORT"]),
    dbname=os.environ["TDX_DB_NAME"], user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
)
conn.autocommit = False

PRESERVE_REASONS = ("data_source_deprecated", "legal", "manual_override")

# ── Before snapshot ──
with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE is_available = FALSE),
               COUNT(*) FILTER (WHERE is_available = FALSE AND disable_reason IS NULL),
               COUNT(*) FILTER (WHERE is_available = FALSE AND disable_reason = ANY(%s))
        FROM aistock_factor_catalog
    """, (list(PRESERVE_REASONS),))
    total, dis_all, dis_null, dis_preserved = cur.fetchone()
print(f"[before] total={total}  disabled_total={dis_all}  disabled_NULL={dis_null}  disabled_preserved={dis_preserved}")

# ── Preview: which factors will be enabled ──
with conn.cursor() as cur:
    cur.execute("""
        SELECT factor_name, source
        FROM aistock_factor_catalog
        WHERE is_available = FALSE
          AND (disable_reason IS NULL OR disable_reason NOT IN %s)
        ORDER BY source, factor_name
        LIMIT 30
    """, (PRESERVE_REASONS,))
    preview = cur.fetchall()
print(f"[preview] will re-enable (first 30):")
for n, s in preview:
    print(f"  [{s}] {n}")

# ── Execute ──
with conn.cursor() as cur:
    cur.execute("""
        UPDATE aistock_factor_catalog
        SET is_available = TRUE,
            disable_reason = NULL,
            disable_at = NULL,
            disable_batch_id = NULL,
            rehab_candidate = FALSE,
            last_rehab_at = NULL
        WHERE is_available = FALSE
          AND (disable_reason IS NULL OR disable_reason NOT IN %s)
    """, (PRESERVE_REASONS,))
    enabled = cur.rowcount
conn.commit()
print(f"[exec] re-enabled {enabled} factors")

# ── After snapshot ──
with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE is_available = TRUE),
               COUNT(*) FILTER (WHERE is_available = FALSE)
        FROM aistock_factor_catalog
    """)
    total, available, still_disabled = cur.fetchone()
print(f"[after]  total={total}  available={available}  still_disabled={still_disabled}")

conn.close()
print("\n[DONE] Phase 0 complete. All v1-era disabled factors re-enabled.")
