import psycopg2
import os

try:
    with open(r'F:\Dev\RD-Agent-main\qe_workspace\qe_exp_b2a5ff59\custom_strategy.py', 'r', encoding='utf-8') as f:
        content = f.read()
        
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    
    cur.execute("UPDATE aistock_strategy_catalog SET source_code = %(code)s WHERE source_code_relpath LIKE %(path)s", 
                {'code': content, 'path': '%enhanced_topk_dropout_v4_copy%'})
    conn.commit()
    print('DB updated rows:', cur.rowcount)
except Exception as e:
    print('DB Error:', e)
