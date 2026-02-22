import psycopg2

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()

    # 1. 确保 stage_mappings 表中有 embedding 这个阶段（如果它不存在）
    cur.execute("SELECT id FROM aistock_llm_stage_mappings WHERE stage_name = 'embedding'")
    if not cur.fetchone():
        print("Inserting 'embedding' stage into aistock_llm_stage_mappings...")
        cur.execute("INSERT INTO aistock_llm_stage_mappings (stage_name, model_id, is_active) VALUES ('embedding', NULL, true)")
    
    # 2. 检查是否有 Qwen3-Embedding-0.6B 这个模型
    cur.execute("SELECT id FROM aistock_llm_models WHERE model_name LIKE '%Qwen3-Embedding-0.6B%'")
    row = cur.fetchone()
    if row:
        model_id = row[0]
        print(f"Found Qwen3-Embedding-0.6B model with ID {model_id}. Updating stage_mappings...")
        cur.execute("UPDATE aistock_llm_stage_mappings SET model_id = %s WHERE stage_name = 'embedding'", (model_id,))
        conn.commit()
        print("DB Update successful.")
    else:
        print("Model Qwen3-Embedding-0.6B not found in DB.")

except Exception as e:
    print('DB Error:', e)
