import os
import psycopg2

file_path = r'F:\Dev\AIstock\backend\routers\rdagent_llm_config.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: get_current_config - read embedding from stage_mappings API response
old_get = '''        # Extract embedding config
        embedding_config = config.get("embedding_config", {})
        embedding_stage_mappings = {}
        if embedding_config.get("litellm_embedding_model"):
            embedding_stage_mappings["default"] = {
                "model": embedding_config["litellm_embedding_model"]
            }'''

new_get = '''        # Extract embedding config (now comes as a regular stage from API if we set it)
        embedding_stage_mappings = {}
        if "embedding" in stage_mappings:
            embedding_stage_mappings["default"] = {
                "model": stage_mappings.pop("embedding")["model"]
            }
        else:
            # Fallback to old embedding_config if it's there
            embedding_config = config.get("embedding_config", {})
            if embedding_config.get("litellm_embedding_model"):
                embedding_stage_mappings["default"] = {
                    "model": embedding_config["litellm_embedding_model"]
                }'''

content = content.replace(old_get, new_get)

# Fix 2: update_config - also update DB aistock_llm_stage_mappings for embedding
old_update = '''            # 5. 处理embedding模型配置
            if config.embedding_model_id:
                cursor.execute("""
                    SELECT m.full_model_id, m.provider_id, m.model_type, m.api_config_id
                    FROM aistock_llm_models m
                    WHERE m.id = %s
                """, (config.embedding_model_id,))

                row = cursor.fetchone()
                if row:
                    full_model_id, provider_id, model_type, model_api_config_id = row
                    if full_model_id:
                        stage_map["embedding"] = {
                            "model": full_model_id,
                        }'''

new_update = '''            # 5. 处理embedding模型配置
            if config.embedding_model_id:
                cursor.execute("""
                    SELECT m.full_model_id, m.provider_id, m.model_type, m.api_config_id
                    FROM aistock_llm_models m
                    WHERE m.id = %s
                """, (config.embedding_model_id,))

                row = cursor.fetchone()
                if row:
                    full_model_id, provider_id, model_type, model_api_config_id = row
                    if full_model_id:
                        stage_map["embedding"] = {
                            "model": config.embedding_model_id,  # Keep ID for DB update
                            "full_model_id": full_model_id,      # Full ID for API
                        }
                        
                        # 同步更新数据库的 embedding 阶段映射
                        cursor.execute("SELECT id FROM aistock_llm_stage_mappings WHERE stage_name = 'embedding'")
                        if cursor.fetchone():
                            cursor.execute("UPDATE aistock_llm_stage_mappings SET model_id = %s, updated_at = CURRENT_TIMESTAMP WHERE stage_name = 'embedding'", (config.embedding_model_id,))
                        else:
                            cursor.execute("INSERT INTO aistock_llm_stage_mappings (stage_name, model_id, is_active) VALUES ('embedding', %s, true)", (config.embedding_model_id,))
'''

content = content.replace(old_update, new_update)

# Fix 3: update_config API payload generation
old_api_payload = '''            # 6. 构建API请求
            api_stage_mappings = []
            for stage_name, stage_config in stage_map.items():
                mapping = {
                    "stage_name": stage_name,
                    "model_id": stage_config["model"],
                }'''

new_api_payload = '''            # 6. 构建API请求
            api_stage_mappings = []
            for stage_name, stage_config in stage_map.items():
                mapping = {
                    "stage_name": stage_name,
                    "model_id": stage_config.get("full_model_id") or stage_config["model"],
                }'''

content = content.replace(old_api_payload, new_api_payload)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated backend API successfully.")
