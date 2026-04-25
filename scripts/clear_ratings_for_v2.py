"""Clear v1 ratings + garbled/v2-sparse classification fields.

KEEP:
  - qe_rating_rule_versions (rule definitions)
  - qe_factor_classification rows + high-value v1 LLM fields
    (category, description, llm_analysis, factor_profile, factor_dimension,
     holding_period_class, data_source_group, update_freq, linearity,
     ic_value/sharpe_value/ann_ret_value as v1 metric snapshot)

CLEAR:
  - qe_factor_official_ratings (TRUNCATE, 784 rows)
  - qe_factor_rating_runs (TRUNCATE, 8 rows)
  - classification v2-sparse fields: direction, signal_mechanism,
    sector_exposure_corr, best_horizon, best_horizon_advantage, horizon_class
  - classification v1 garbled fields: grade, grade_reason, classification_reason
"""
import os
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
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
conn.autocommit = False

# ── Before snapshot ──
def snap(label):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM qe_factor_official_ratings"); r = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM qe_factor_rating_runs"); u = cur.fetchone()[0]
        cur.execute("""SELECT
            COUNT(*) FILTER (WHERE direction IS NOT NULL),
            COUNT(*) FILTER (WHERE signal_mechanism IS NOT NULL),
            COUNT(*) FILTER (WHERE sector_exposure_corr IS NOT NULL),
            COUNT(*) FILTER (WHERE best_horizon IS NOT NULL),
            COUNT(*) FILTER (WHERE grade_reason IS NOT NULL),
            COUNT(*) FILTER (WHERE classification_reason IS NOT NULL),
            COUNT(*) FILTER (WHERE category IS NOT NULL),
            COUNT(*) FILTER (WHERE description IS NOT NULL)
            FROM qe_factor_classification""")
        cls = cur.fetchone()
    print(f"[{label}] ratings={r}  runs={u}")
    print(f"[{label}] classification — direction={cls[0]} sig_mech={cls[1]} sector_exp={cls[2]} best_horizon={cls[3]}")
    print(f"[{label}] classification — grade_reason={cls[4]} class_reason={cls[5]} category={cls[6]} description={cls[7]}")

snap("before")
print()

try:
    with conn.cursor() as cur:
        # Truncate both in one statement to satisfy FK constraint
        cur.execute("TRUNCATE TABLE qe_factor_official_ratings, qe_factor_rating_runs")
        print(f"[1-2] TRUNCATE qe_factor_official_ratings, qe_factor_rating_runs → ok")

        # Clear v2-sparse + v1 garbled fields; keep LLM knowledge
        cur.execute("""
            UPDATE qe_factor_classification SET
                -- v2 sparse fields
                direction = NULL,
                signal_mechanism = NULL,
                sector_exposure_corr = NULL,
                best_horizon = NULL,
                best_horizon_advantage = NULL,
                horizon_class = NULL,
                -- v1 garbled/stale fields
                grade = NULL,
                grade_reason = NULL,
                classification_reason = NULL
        """)
        updated = cur.rowcount
        print(f"[3] UPDATE qe_factor_classification clear 9 cols → {updated} rows")

    conn.commit()
    print("\n[commit] OK\n")
except Exception as e:
    conn.rollback()
    print(f"[rollback] {e}")
    raise

snap("after")
conn.close()
print("\n[DONE] 评级数据清理完成。可在UI触发 v2 全量评级。")
