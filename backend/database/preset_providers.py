"""
预置LLM服务商数据
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 加载AIstock的.env文件
aistock_env_path = project_root.parent / ".env"
if aistock_env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(aistock_env_path)

from db.pg_pool import get_conn


def preset_providers():
    """预置4个服务商：DeepSeek、硅基流动、阿里云百炼、Claude"""
    
    providers = [
        {
            "provider_name": "deepseek",
            "display_name": "DeepSeek",
            "api_base_url": "https://api.deepseek.com",
            "litellm_prefix": "deepseek",
            "supports_chat": True,
            "supports_embedding": False,
            "supports_reasoner": True
        },
        {
            "provider_name": "siliconflow",
            "display_name": "硅基流动",
            "api_base_url": "https://api.siliconflow.cn/v1",
            "litellm_prefix": "openai",
            "supports_chat": True,
            "supports_embedding": True,
            "supports_reasoner": False
        },
        {
            "provider_name": "dashscope",
            "display_name": "阿里云百炼",
            "api_base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "litellm_prefix": "openai",
            "supports_chat": True,
            "supports_embedding": True,
            "supports_reasoner": False
        },
        {
            "provider_name": "anthropic",
            "display_name": "Claude",
            "api_base_url": "https://api.anthropic.com",
            "litellm_prefix": "anthropic",
            "supports_chat": True,
            "supports_embedding": False,
            "supports_reasoner": True
        }
    ]
    
    print("开始预置服务商...")
    
    with get_conn() as conn:
        cursor = conn.cursor()
        
        for provider in providers:
            # 检查是否已存在
            cursor.execute(
                "SELECT id FROM aistock_llm_providers WHERE provider_name = %s",
                (provider["provider_name"],)
            )
            row = cursor.fetchone()
            
            if row:
                # 更新现有服务商
                cursor.execute("""
                    UPDATE aistock_llm_providers
                    SET display_name = %s, api_base_url = %s, litellm_prefix = %s,
                        supports_chat = %s, supports_embedding = %s, supports_reasoner = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE provider_name = %s
                """, (
                    provider["display_name"],
                    provider["api_base_url"],
                    provider["litellm_prefix"],
                    provider["supports_chat"],
                    provider["supports_embedding"],
                    provider["supports_reasoner"],
                    provider["provider_name"]
                ))
                print(f"✓ 更新服务商: {provider['display_name']}")
            else:
                # 插入新服务商
                cursor.execute("""
                    INSERT INTO aistock_llm_providers
                    (provider_name, display_name, api_base_url, litellm_prefix,
                     supports_chat, supports_embedding, supports_reasoner)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    provider["provider_name"],
                    provider["display_name"],
                    provider["api_base_url"],
                    provider["litellm_prefix"],
                    provider["supports_chat"],
                    provider["supports_embedding"],
                    provider["supports_reasoner"]
                ))
                print(f"✓ 插入服务商: {provider['display_name']}")
        
        conn.commit()
        cursor.close()
    
    print("\n✓ 服务商预置完成")


if __name__ == "__main__":
    try:
        preset_providers()
    except Exception as e:
        print(f"❌ 预置失败: {e}")
        import traceback
        traceback.print_exc()
