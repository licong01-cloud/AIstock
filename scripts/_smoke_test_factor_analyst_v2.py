"""Smoke test: run factor_analyst v2 on one factor and verify 3 new fields persist."""
import os, sys
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

sys.path.insert(0, str(REPO_ROOT))

import psycopg2
from backend.services.quantevolver.factor_analyst import FactorAnalyst

# Pick a known factor
fa = FactorAnalyst()

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ["TDX_DB_PORT"]),
    dbname=os.environ["TDX_DB_NAME"], user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
)
with conn.cursor() as cur:
    cur.execute("""
        SELECT factor_name, source FROM aistock_factor_catalog
        WHERE factor_name = 'neg_IntradayVolatility_5D' LIMIT 1
    """)
    row = cur.fetchone()
if not row:
    print("[SKIP] factor not found")
    sys.exit(0)

factor_name, factor_source = row
print(f"[TEST] analyzing {factor_name} (source={factor_source}) with use_llm=True")
r = fa.analyze_single_factor(factor_name, factor_source, use_llm=True)
print(f"       result ok={r.get('ok')}")
print(f"       category={r.get('category')}  grade={r.get('grade')}")

# Read back classification row to verify 3 new cols
with conn.cursor() as cur:
    cur.execute("""
        SELECT direction, signal_mechanism, sector_exposure_corr
        FROM qe_factor_classification
        WHERE factor_name = %s AND factor_source = %s
    """, (factor_name, factor_source))
    row2 = cur.fetchone()
conn.close()

if row2:
    d, sm, sc = row2
    print(f"[DB]  direction={d}  signal_mechanism={sm}  sector_exposure_corr={sc}")
else:
    print("[DB]  no classification row found")
