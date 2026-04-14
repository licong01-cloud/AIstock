import sys
sys.path.insert(0, '.')
from backend.services.quantevolver.factor_code_transformer import FactorCodeTransformer
import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock', user='postgres', password='lc78080808')
cur = conn.cursor()
cur.execute("""
    SELECT factor_name, code_text
    FROM aistock_factor_catalog
    WHERE transformation_status = 'FAILED'
    ORDER BY factor_name
    LIMIT 10
""")
rows = cur.fetchall()
conn.close()

print(f"Testing {len(rows)} failed factors")
t = FactorCodeTransformer()

for factor_name, [r[1]:
        code_text = r[1]
    result = t.transform(code_text, factor_name)
    try:
        compile(result.transformed_code, '<test>', 'exec')
        status = 'PASS'
    except SyntaxError as e:
        status = 'FAIL'
        print(f"{factor_name}: {status}")
        print(f"  syntax error: {e}")
        lines = result.transformed_code.split('\n')
        print(f"  total lines: {len(lines)}")
        for i in range(min(5, len(lines))):
            print(f"  {i+1}: {lines[i]}")
        print("  ...")
    if status == 'PASS':
        print(f"{factor_name}: {status}")
