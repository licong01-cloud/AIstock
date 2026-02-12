"""
从现有.env文件导入API配置到数据库
功能：解析RDAgent的.env文件，提取API配置并导入到aistock_llm_api_configs表
"""

import re
import sys
from pathlib import Path

from dotenv import load_dotenv

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent  # 指向AIstock/backend
sys.path.insert(0, str(project_root))

from db.pg_pool import get_conn

# 加载AIstock的.env文件以获取数据库连接信息
aistock_env_path = project_root.parent / ".env"
if aistock_env_path.exists():
    load_dotenv(aistock_env_path)

# RDAgent .env文件路径
RDAGENT_ENV_PATH = Path("F:/Dev/RD-Agent-main/.env")


def extract_env_var(content: str, var_name: str) -> str | None:
    """从.env文件内容中提取环境变量值"""
    pattern = rf'^\s*{var_name}=(.+)$'
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        value = match.group(1).strip().strip('"').strip("'")
        return value if value else None
    return None


def import_api_configs():
    """从.env文件导入API配置到数据库"""
    print("开始从.env文件导入API配置...")
    
    if not RDAGENT_ENV_PATH.exists():
        print(f"错误：找不到RDAgent .env文件：{RDAGENT_ENV_PATH}")
        return
    
    # 读取.env文件
    with open(RDAGENT_ENV_PATH, 'r', encoding='utf-8') as f:
        env_content = f.read()
    
    with get_conn() as conn:
        cursor = conn.cursor()
        
        # 定义API配置映射
        api_configs = [
            # DeepSeek配置
            {
                'provider_name': 'deepseek',
                'api_base': None,  # DeepSeek使用默认endpoint
                'api_key': extract_env_var(env_content, 'DEEPSEEK_API_KEY'),
                'env_api_base_name': 'DEEPSEEK_API_BASE',
                'env_api_key_name': 'DEEPSEEK_API_KEY',
                'config_purpose': 'chat',
                'description': 'DeepSeek Chat模型API配置'
            },
            # 阿里云百炼/GLM配置（Chat）
            {
                'provider_name': 'openai',
                'api_base': extract_env_var(env_content, 'OPENAI_API_BASE'),
                'api_key': extract_env_var(env_content, 'OPENAI_API_KEY'),
                'env_api_base_name': 'OPENAI_API_BASE',
                'env_api_key_name': 'OPENAI_API_KEY',
                'config_purpose': 'chat',
                'description': '阿里云百炼GLM模型API配置（通用）'
            },
            # 阿里云百炼/GLM配置（Chat专用）
            {
                'provider_name': 'openai',
                'api_base': extract_env_var(env_content, 'CHAT_OPENAI_API_BASE'),
                'api_key': extract_env_var(env_content, 'CHAT_OPENAI_API_KEY'),
                'env_api_base_name': 'CHAT_OPENAI_API_BASE',
                'env_api_key_name': 'CHAT_OPENAI_API_KEY',
                'config_purpose': 'chat',
                'description': '阿里云百炼GLM模型API配置（Chat专用）',
                'priority': 1  # 更高优先级
            },
            # 硅基流动配置（Embedding）
            {
                'provider_name': 'openai',
                'api_base': extract_env_var(env_content, 'EMBEDDING_API_BASE'),
                'api_key': extract_env_var(env_content, 'EMBEDDING_API_KEY'),
                'env_api_base_name': 'EMBEDDING_API_BASE',
                'env_api_key_name': 'EMBEDDING_API_KEY',
                'config_purpose': 'embedding',
                'description': '硅基流动Embedding模型API配置'
            },
        ]
        
        # 插入API配置
        for config in api_configs:
            # 跳过没有API Key的配置
            if not config['api_key']:
                print(f"⚠ 跳过 {config['provider_name']} - {config['config_purpose']}：缺少API Key")
                continue
            
            # 查询provider_id
            cursor.execute(
                "SELECT id FROM aistock_llm_providers WHERE provider_name = %s",
                (config['provider_name'],)
            )
            row = cursor.fetchone()
            if not row:
                print(f"⚠ 跳过 {config['provider_name']}：服务商不存在")
                continue
            
            provider_id = row[0]
            
            # 检查是否已存在相同配置
            cursor.execute("""
                SELECT id FROM aistock_llm_api_configs 
                WHERE provider_id = %s AND config_purpose = %s AND is_active = true
            """, (provider_id, config['config_purpose']))
            
            if cursor.fetchone():
                print(f"⚠ 跳过 {config['provider_name']} - {config['config_purpose']}：配置已存在")
                continue
            
            # 插入新配置
            cursor.execute("""
                INSERT INTO aistock_llm_api_configs 
                (provider_id, api_base, api_key, env_api_base_name, env_api_key_name, 
                 config_purpose, priority, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                provider_id,
                config['api_base'] or '',
                config['api_key'],
                config['env_api_base_name'],
                config['env_api_key_name'],
                config['config_purpose'],
                config.get('priority', 0),
                config['description']
            ))
            
            config_id = cursor.fetchone()[0]
            print(f"✓ 导入 {config['provider_name']} - {config['config_purpose']} (ID: {config_id})")
        
        # 关联模型与API配置
        print("\n开始关联模型与API配置...")
        
        # 获取所有模型
        cursor.execute("""
            SELECT m.id, m.full_model_id, m.model_type, p.provider_name
            FROM aistock_llm_models m
            JOIN aistock_llm_providers p ON m.provider_id = p.id
            WHERE m.is_active = true
        """)
        
        models = cursor.fetchall()
        
        for model_id, full_model_id, model_type, provider_name in models:
            # 根据模型类型和服务商确定应该使用哪个API配置
            config_purpose = 'embedding' if model_type == 'embedding' else 'chat'
            
            # 查询对应的API配置
            cursor.execute("""
                SELECT id FROM aistock_llm_api_configs
                WHERE provider_id = (SELECT id FROM aistock_llm_providers WHERE provider_name = %s)
                AND config_purpose = %s
                AND is_active = true
                ORDER BY priority DESC
                LIMIT 1
            """, (provider_name, config_purpose))
            
            row = cursor.fetchone()
            if row:
                api_config_id = row[0]
                
                # 更新模型的api_config_id
                cursor.execute("""
                    UPDATE aistock_llm_models
                    SET api_config_id = %s
                    WHERE id = %s
                """, (api_config_id, model_id))
                
                print(f"✓ 关联模型 {full_model_id} → API配置 {api_config_id}")
            else:
                print(f"⚠ 模型 {full_model_id} 未找到匹配的API配置")
        
        conn.commit()
        cursor.close()
    
    print("\n✓ API配置导入完成")


if __name__ == "__main__":
    import_api_configs()
