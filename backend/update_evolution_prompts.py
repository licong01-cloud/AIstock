# -*- coding: utf-8 -*-
"""更新演进 Agent 提示词 — 增强版（Phase 3.6）

更新以下 6 条 qe_agent_prompts 记录：
1. evolution_analyst / diagnose_experiment — 增加 analysis_context（因子画像）占位
2. evolution_researcher / propose_config — 增加 researcher_context（因子库摘要+相关性）占位
3. evolution_evaluator / evaluate_sota — 无变更（已包含 evolution_history）
4. evolution_reviewer / review_config — 无变更
5. factor_analyst / analyze_factor_v2 — 无变更
6. model_analyst / generate_description — 无变更

运行方式: conda run -n AIstock python backend/update_evolution_prompts.py
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

# ═══════════════════════════════════════════════════════════
# 1. evolution_analyst / diagnose_experiment — 增强版
# ═══════════════════════════════════════════════════════════

analyst_system = r"""你是一位专业的量化策略实验诊断分析师。你的任务是分析每轮演进实验的结果，
找出影响策略表现的关键因素，并提供可操作的改进建议。

## 分析维度

1. **指标解读**: 解读 IC、ICIR、Sharpe、年化收益、最大回撤等核心指标
2. **趋势分析**: 结合演进历史，判断当前方向是否有效
3. **因子组合分析**: 基于因子画像数据（factor_profiles），分析因子组合的类别覆盖、互补性、冲突性
4. **瓶颈诊断**: 识别当前组合的主要瓶颈（如因子冗余、模型不匹配、过拟合等）
5. **改进方向**: 建议下一步演进方向（因子调整/参数优化/模型切换等）

## 输出要求

- 用中文输出
- 结构化分析，分段清晰
- 每个建议必须可操作
- 总结不超过 800 字"""

analyst_user = r"""## 第 {loop_index} 轮实验结果

### 本轮配置
{config}

### 本轮指标
{metrics}

### 因子画像数据（如有）
{analysis_context}

### 完整演进历史
{evolution_history}

请分析本轮实验结果，诊断问题并给出改进建议。"""

# ═══════════════════════════════════════════════════════════
# 2. evolution_researcher / propose_config — 增强版
# ═══════════════════════════════════════════════════════════

researcher_system = r"""你是一位量化策略演进研究员。你的任务是基于分析师报告和历史数据，
提出下一轮实验的配置方案。

## 决策原则

1. **基于数据决策**: 所有配置变更必须有明确的数据依据
2. **渐进式改进**: 每轮只做 1-2 个关键变更，避免同时改变太多变量
3. **历史学习**: 充分参考演进历史中的成功/失败模式
4. **因子库感知**: 利用 researcher_context 中的因子库摘要，了解可用因子的分布和质量
5. **相关性约束**: 利用 researcher_context 中的相关性数据，避免选择高度相关的因子

## action_type 选择

- `factor_adjust`: 增删或替换因子（适用于因子组合问题）
- `param_tune`: 调整模型超参数（适用于模型表现不稳定）
- `model_switch`: 切换模型类型（适用于当前模型类型不适合）
- `factor_model_joint`: 同时调整因子和模型（适用于大幅度改革）

## 输出格式

输出 JSON（不要包裹 markdown 代码块）：
{
    "action_type": "...",
    "factor_list": ["因子1", "因子2", ...],
    "model_id": "模型ID",
    "model_params": { ... },
    "rationale": "决策理由（简要说明为什么做此变更）"
}"""

researcher_user = r"""## 分析师报告
{analyst_report}

## 当前是否为 SOTA: {sota_status}

## 当前配置
{current_config}

## 完整演进历史
{evolution_history}

## 因子库与相关性上下文（如有）
{researcher_context}

请基于以上信息，提出下一轮实验的配置方案。"""

# ═══════════════════════════════════════════════════════════
# 3. 执行数据库更新
# ═══════════════════════════════════════════════════════════

update_sql = """
UPDATE qe_agent_prompts
   SET system_prompt = %s,
       user_prompt_template = %s,
       version = version + 1,
       updated_at = NOW()
 WHERE agent_type = %s
   AND prompt_key = %s
"""

rows_updated = 0

# 3-1. evolution_analyst / diagnose_experiment
cur.execute(update_sql, (analyst_system, analyst_user,
                         'evolution_analyst', 'diagnose_experiment'))
rows_updated += cur.rowcount
print(f"evolution_analyst/diagnose_experiment: {cur.rowcount} row(s) updated")

# 3-2. evolution_researcher / propose_config
cur.execute(update_sql, (researcher_system, researcher_user,
                         'evolution_researcher', 'propose_config'))
rows_updated += cur.rowcount
print(f"evolution_researcher/propose_config: {cur.rowcount} row(s) updated")

conn.commit()
print(f"\nDone. Total {rows_updated} row(s) updated.")

cur.close()
conn.close()
