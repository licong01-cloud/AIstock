# -*- coding: utf-8 -*-
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock', user='postgres', password=os.environ.get('TDX_DB_PASSWORD', 'lc78080808'))
cur = conn.cursor()

system_prompt = '''你是一个顶级的量化投资组合架构师。
你的任务是根据用户的自然语言需求，从提供的因子库和模型库中挑选最合适的因子和模型，构建一个量化投资组合。
返回JSON格式：
{
    "factors": ["因子名1", "因子名2"],
    "model_id": "选择的模型ID",
    "rationale": "设计组合的理由和分析（100字左右）",
    "strategy_params": {
        "topk": 50,
        "n_drop": 5
    }
}
要求：
1. 因子数量不能超过 {max_factors} 个。
2. 只从 {available_factors} 中选择因子。
3. 只从 {model_summary} 中选择模型。
4. JSON 必须是合法的，不要输出任何额外的解释文本。'''

user_prompt_template = '''用户需求：{user_requirement}
最大因子数：{max_factors}

因子库概览：
{factor_summary}

可用因子列表（请只从中选择）：
{available_factors}

可用模型列表（请只从中选择）：
{model_summary}

请根据用户需求，挑选因子和模型，生成最优的组合配置。'''

cur.execute('''
INSERT INTO qe_agent_prompts (agent_type, prompt_key, display_name, description, system_prompt, user_prompt_template, is_active, version)
VALUES ('portfolio_architect', 'smart_select', '智能组合选择', '用于根据用户需求智能挑选因子和模型组合', %s, %s, true, 1)
ON CONFLICT (agent_type, prompt_key) DO UPDATE SET 
    system_prompt = EXCLUDED.system_prompt,
    user_prompt_template = EXCLUDED.user_prompt_template;
''', (system_prompt, user_prompt_template))

conn.commit()
cur.close()
conn.close()
print('Inserted smart_select prompt')
