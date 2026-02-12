"""
初始化RD-Agent LLM配置数据库表并从.env文件导入初始数据
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 加载AIstock的.env文件以获取数据库连接信息
aistock_env_path = project_root.parent / ".env"
if aistock_env_path.exists():
    load_dotenv(aistock_env_path)

from db.pg_pool import get_conn


def create_tables():
    """创建数据库表"""
    print("开始创建数据库表...")
    
    with get_conn() as conn:
        cursor = conn.cursor()
        
        # 创建LLM服务商表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aistock_llm_providers (
                id SERIAL PRIMARY KEY,
                provider_name VARCHAR(100) NOT NULL UNIQUE,
                display_name VARCHAR(200) NOT NULL,
                api_base_url VARCHAR(500),
                litellm_prefix VARCHAR(50),
                supports_chat BOOLEAN DEFAULT true,
                supports_embedding BOOLEAN DEFAULT false,
                supports_reasoner BOOLEAN DEFAULT false,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ 创建 aistock_llm_providers 表")
        
        # 创建LLM模型表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aistock_llm_models (
                id SERIAL PRIMARY KEY,
                provider_id INTEGER NOT NULL REFERENCES aistock_llm_providers(id) ON DELETE CASCADE,
                model_name VARCHAR(200) NOT NULL,
                display_name VARCHAR(300) NOT NULL,
                full_model_id VARCHAR(300) NOT NULL,
                model_type VARCHAR(50) NOT NULL,
                model_category VARCHAR(50),
                description TEXT,
                is_verified BOOLEAN DEFAULT false,
                last_verified_at TIMESTAMP,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider_id, model_name)
            )
        """)
        print("✓ 创建 aistock_llm_models 表")
        
        # 创建阶段映射表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aistock_llm_stage_mappings (
                id SERIAL PRIMARY KEY,
                stage_name VARCHAR(100) NOT NULL UNIQUE,
                model_id INTEGER REFERENCES aistock_llm_models(id) ON DELETE SET NULL,
                temperature DECIMAL(3, 2),
                max_tokens INTEGER,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ 创建 aistock_llm_stage_mappings 表")
        
        # 创建变更记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aistock_llm_config_change_log (
                id SERIAL PRIMARY KEY,
                stage_name VARCHAR(100) NOT NULL,
                old_model_id INTEGER,
                new_model_id INTEGER,
                change_reason TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                changed_by VARCHAR(100) DEFAULT 'system'
            )
        """)
        print("✓ 创建 aistock_llm_config_change_log 表")
        
        conn.commit()
        cursor.close()
    
    print("✓ 所有表创建完成")


def insert_initial_data():
    """从RD-Agent .env文件读取并插入初始数据"""
    print("\n开始从.env文件读取初始数据...")
    
    # 加载RD-Agent的.env文件
    rdagent_env_path = Path("F:/Dev/RD-Agent-main/.env")
    if not rdagent_env_path.exists():
        print(f"❌ 未找到RD-Agent .env文件: {rdagent_env_path}")
        return
    
    load_dotenv(rdagent_env_path)
    
    # 从.env读取配置
    chat_model = os.getenv("CHAT_MODEL", "")
    embedding_model = os.getenv("EMBEDDING_MODEL", "")
    litellm_map_str = os.getenv("LITELLM_CHAT_MODEL_MAP", "")
    
    print(f"CHAT_MODEL: {chat_model}")
    print(f"EMBEDDING_MODEL: {embedding_model}")
    
    # 解析模型信息
    providers_models = {}
    stage_mappings = {}
    
    # 解析CHAT_MODEL
    if chat_model and "/" in chat_model:
        provider, model = chat_model.split("/", 1)
        if provider not in providers_models:
            providers_models[provider] = []
        providers_models[provider].append({
            "model_name": model,
            "model_type": "chat",
            "full_model_id": chat_model,
            "description": "默认对话模型"
        })
    
    # 解析EMBEDDING_MODEL
    if embedding_model and "/" in embedding_model:
        provider, model = embedding_model.split("/", 1)
        if provider not in providers_models:
            providers_models[provider] = []
        providers_models[provider].append({
            "model_name": model,
            "model_type": "embedding",
            "full_model_id": embedding_model,
            "description": "嵌入模型"
        })
    
    # 解析LITELLM_CHAT_MODEL_MAP JSON配置
    if litellm_map_str:
        try:
            import json
            litellm_map = json.loads(litellm_map_str)
            print(f"\n解析LITELLM_CHAT_MODEL_MAP: {len(litellm_map)} 个阶段配置")
            
            for stage, config in litellm_map.items():
                model_id = config.get("model", "")
                temperature = config.get("temperature")
                max_tokens = config.get("max_tokens")
                
                if model_id and "/" in model_id:
                    provider, model = model_id.split("/", 1)
                    if provider not in providers_models:
                        providers_models[provider] = []
                    
                    # 避免重复
                    if not any(m["model_name"] == model for m in providers_models[provider]):
                        providers_models[provider].append({
                            "model_name": model,
                            "model_type": "chat",
                            "full_model_id": model_id,
                            "description": f"{stage}阶段模型"
                        })
                    
                    # 记录阶段映射
                    stage_mappings[stage] = {
                        "model_id": model_id,
                        "temperature": float(temperature) if temperature else None,
                        "max_tokens": int(max_tokens) if max_tokens else None
                    }
                    print(f"  - {stage}: {model_id} (temp={temperature}, max_tokens={max_tokens})")
        except Exception as e:
            print(f"⚠️  解析LITELLM_CHAT_MODEL_MAP失败: {e}")
    
    print(f"\n发现 {len(providers_models)} 个服务商:")
    for provider in providers_models.keys():
        print(f"  - {provider}: {len(providers_models[provider])} 个模型")
    
    with get_conn() as conn:
        cursor = conn.cursor()
        
        # 插入服务商
        provider_ids = {}
        for provider_name in providers_models.keys():
            # 检查是否已存在
            cursor.execute(
                "SELECT id FROM aistock_llm_providers WHERE provider_name = %s",
                (provider_name,)
            )
            row = cursor.fetchone()
            
            if row:
                provider_ids[provider_name] = row[0]
                print(f"✓ 服务商已存在: {provider_name} (ID: {row[0]})")
            else:
                cursor.execute("""
                    INSERT INTO aistock_llm_providers 
                    (provider_name, display_name, litellm_prefix, supports_chat, supports_embedding)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    provider_name,
                    provider_name.upper(),
                    provider_name,
                    True,
                    provider_name in ["openai", "text-embedding"]
                ))
                provider_id = cursor.fetchone()[0]
                provider_ids[provider_name] = provider_id
                print(f"✓ 插入服务商: {provider_name} (ID: {provider_id})")
        
        # 插入模型并记录model_id映射
        model_count = 0
        model_id_map = {}  # full_model_id -> database_id
        
        for provider_name, models in providers_models.items():
            provider_id = provider_ids[provider_name]
            
            for model_info in models:
                # 检查是否已存在
                cursor.execute(
                    "SELECT id FROM aistock_llm_models WHERE provider_id = %s AND model_name = %s",
                    (provider_id, model_info["model_name"])
                )
                row = cursor.fetchone()
                
                if row:
                    model_id_map[model_info["full_model_id"]] = row[0]
                else:
                    cursor.execute("""
                        INSERT INTO aistock_llm_models
                        (provider_id, model_name, display_name, full_model_id, model_type, model_category, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        provider_id,
                        model_info["model_name"],
                        model_info["model_name"],
                        model_info["full_model_id"],
                        model_info["model_type"],
                        "general",
                        model_info.get("description")
                    ))
                    db_model_id = cursor.fetchone()[0]
                    model_id_map[model_info["full_model_id"]] = db_model_id
                    model_count += 1
                    print(f"  ✓ 插入模型: {model_info['full_model_id']}")
        
        print(f"\n✓ 共插入 {model_count} 个模型")
        
        # 插入阶段映射
        if stage_mappings:
            print(f"\n插入 {len(stage_mappings)} 个阶段映射...")
            for stage, mapping in stage_mappings.items():
                model_id_str = mapping["model_id"]
                db_model_id = model_id_map.get(model_id_str)
                
                if db_model_id:
                    # 检查是否已存在
                    cursor.execute(
                        "SELECT id FROM aistock_llm_stage_mappings WHERE stage_name = %s",
                        (stage,)
                    )
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO aistock_llm_stage_mappings
                            (stage_name, model_id, temperature, max_tokens)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            stage,
                            db_model_id,
                            mapping["temperature"],
                            mapping["max_tokens"]
                        ))
                        print(f"  ✓ 插入阶段映射: {stage} -> {model_id_str}")
        
        conn.commit()
        cursor.close()
    
    print("\n✓ 初始数据导入完成")


if __name__ == "__main__":
    try:
        print("=" * 60)
        print("RD-Agent LLM配置数据库初始化")
        print("=" * 60)
        
        create_tables()
        insert_initial_data()
        
        print("\n" + "=" * 60)
        print("✓ 数据库初始化完成！")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
