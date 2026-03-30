# -*- coding: utf-8 -*-
"""注册因子相关性分析 Agent 的初始提示词

注册 1 条 qe_agent_prompts 记录：
  correlation_analysis / analyze_pair — 因子对相关性 LLM 分析

运行方式: conda run -n AIstock python backend/register_correlation_prompts.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import psycopg2
from backend.db.pg_pool import _db_cfg

conn = psycopg2.connect(**_db_cfg())
cur = conn.cursor()

upsert_sql = """
INSERT INTO qe_agent_prompts
    (agent_type, prompt_key, display_name, description, system_prompt, user_prompt_template, is_active, version)
VALUES (%s, %s, %s, %s, %s, %s, true, 1)
ON CONFLICT (agent_type, prompt_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    system_prompt = EXCLUDED.system_prompt,
    user_prompt_template = EXCLUDED.user_prompt_template,
    version = qe_agent_prompts.version + 1,
    updated_at = NOW()
"""

prompts = []

# ══════════════════════════════════════════════════════════
# correlation_analysis / analyze_pair
# ══════════════════════════════════════════════════════════
prompts.append((
    "correlation_analysis", "analyze_pair",
    "因子相关性分析", "分析因子对之间高相关性的成因，从代码逻辑、数学关系和金融含义三个维度给出分析和处理建议",
    # ── system_prompt ──
    r"""你是量化金融因子分析专家。你的任务是分析两个因子之间高相关性的成因，并给出处理建议。

## 分析维度

1. **代码逻辑比较**：两个因子的计算过程是否本质相同或高度相似？使用了哪些共同的原始特征（如 close, volume, high, low 等）？计算步骤有何异同？
2. **数学关系**：两个因子之间是否存在线性变换、单调变换、子集关系、或其他数学等价关系？
3. **金融含义**：两个因子是否捕捉了相同的市场信号（如都反映动量/波动率/流动性/价值等）？信号维度是否完全重叠？
4. **处理建议**：基于指标对比，应保留哪个因子？是否可以合并？还是两个都有独立价值应保留？

## 输出要求

- 使用中文回答
- 结构清晰，分维度阐述
- 最后给出明确的结论和置信度（高/中/低）
- 总字数控制在 600 字以内""",
    # ── user_prompt_template ──
    r"""## 因子 A: {factor_a_name}
- 描述: {factor_a_description}
- 评级: {factor_a_grade} | 分类: {factor_a_category}
- IC: {factor_a_ic} | Sharpe: {factor_a_sharpe} | 年化收益: {factor_a_return} | 最大回撤: {factor_a_drawdown}
- 源代码:
```python
{factor_a_code}
```

## 因子 B: {factor_b_name}
- 描述: {factor_b_description}
- 评级: {factor_b_grade} | 分类: {factor_b_category}
- IC: {factor_b_ic} | Sharpe: {factor_b_sharpe} | 年化收益: {factor_b_return} | 最大回撤: {factor_b_drawdown}
- 源代码:
```python
{factor_b_code}
```

## 相关性
- Spearman 截面相关系数 (EWMA): {correlation_value}
- 计算窗口: {window_days} 天

请从代码逻辑、数学关系和金融含义三个维度分析这两个因子的高相关性成因，并给出保留/剔除建议。""",
))

# ── 执行 ──
for p in prompts:
    cur.execute(upsert_sql, p)
    print(f"  [OK] {p[0]}/{p[1]} — {p[2]}")

conn.commit()
cur.close()
conn.close()
print(f"\n完成: 共注册 {len(prompts)} 条提示词")
