import psycopg2

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    
    with open(r'F:\Dev\AIstock\rdagent_assets\qe_strategies\enhanced_topk_dropout_v4_copy.py', 'r', encoding='utf-8') as f:
        content = f.read()
        
    cur.execute("UPDATE aistock_strategy_catalog SET source_code = %s WHERE file_path LIKE '%enhanced_topk_dropout_v4_copy.py%'", (content,))
    conn.commit()
    print('Updated DB rows:', cur.rowcount)
except Exception as e:
    print('DB Error:', e)
