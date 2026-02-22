import psycopg2

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cur.fetchall()
    print("Tables containing 'llm':")
    for table in tables:
        if 'llm' in table[0].lower():
            print(table[0])
            
    print("\nChecking rdagent_llm_config_v2:")
    cur.execute("SELECT * FROM rdagent_llm_config_v2 LIMIT 1")
    row = cur.fetchone()
    if row:
        cols = [desc[0] for desc in cur.description]
        for col, val in zip(cols, row):
            if col in ['stage_mappings', 'embedding_model_id', 'llm_chat_model_map']:
                print(f"{col}: {val}")
except Exception as e:
    print('DB Error:', e)
