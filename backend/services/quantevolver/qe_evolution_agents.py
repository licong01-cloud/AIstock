import logging
import json
import asyncio
from typing import Dict, Any, Optional

from .llm_client import get_llm_kwargs

try:
    import litellm
except ImportError:
    litellm = None

logger = logging.getLogger(__name__)

class EvolutionAgents:
    """
    包含自动演进流程中使用的各个 Agent 角色调用逻辑。
    负责通过 llm_client 调取大模型进行分析和决策。
    """
    
    def _call_llm(self, agent_type: str, system_prompt: str, user_prompt: str) -> str:
        if litellm is None:
            logger.warning("litellm is not installed, returning mock response.")
            return "Mock response due to missing litellm."
            
        kwargs = get_llm_kwargs(agent_type)
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        try:
            response = litellm.completion(
                messages=messages,
                **kwargs
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"LLM call failed for {agent_type}: {e}")
            return f"LLM Error: {str(e)}"
            
    async def async_call_llm(self, agent_type: str, system_prompt: str, user_prompt: str) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._call_llm, agent_type, system_prompt, user_prompt)
        
    async def run_analyst(self, loop_index: int, config: Dict[str, Any], metrics: Dict[str, Any]) -> str:
        """实验诊断分析师"""
        system_prompt = "你是一位量化实验诊断分析师(Analyst)。你的任务是根据量化模型回测的配置和结果指标，诊断当前组合的瓶颈和表现。请给出简短、专业的结论。"
        user_prompt = f"""
请分析以下实验结果：
- Loop 轮次: {loop_index}
- 实验配置: {json.dumps(config, ensure_ascii=False)}
- 回测核心指标: {json.dumps(metrics, ensure_ascii=False)}

请给出一段简明扼要的诊断报告（100字左右），指出表现优异的地方以及可能存在的过拟合或特征衰减风险。
"""
        return await self.async_call_llm("analyst", system_prompt, user_prompt)

    async def run_evaluator(self, current_metrics: Dict[str, Any], historical_sota_metrics: Optional[Dict[str, Any]]) -> bool:
        """
        SOTA 评估官
        """
        if not historical_sota_metrics:
            return True # 第一轮默认是 SOTA
            
        # 这里用纯规则判断，也可以用大模型。为了稳定，建议先用规则核心指标判断
        cur_ic = current_metrics.get("IC", 0)
        sota_ic = historical_sota_metrics.get("IC", 0)
        cur_icir = current_metrics.get("ICIR", 0)
        sota_icir = historical_sota_metrics.get("ICIR", 0)
        
        # SOTA 规则：IC 提升 或者 IC 差距在 0.005 以内但 ICIR 显著提升
        if cur_ic > sota_ic + 0.002:
            return True
        if abs(cur_ic - sota_ic) <= 0.005 and cur_icir > sota_icir + 0.05:
            return True
            
        return False

    async def run_researcher(self, analyst_report: str, sota_status: bool, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        演进策略研究员
        """
        system_prompt = "你是一位高级量化策略研究员(Researcher)。你的任务是根据上一轮的诊断报告，决策下一轮的配置方案（决定是调整因子、调整超参，还是更换模型）。只输出 JSON 格式的更新配置，不要输出其他废话。"
        user_prompt = f"""
上一轮诊断意见: {analyst_report}
上一轮是否达到 SOTA (最优): {sota_status}
当前基础配置: {json.dumps(current_config, ensure_ascii=False)}

请给出下一轮建议的实验配置(格式必须为严格的 JSON)。
如果上一轮过拟合，尝试增加正则化(reg_lambda/reg_alpha)或减少树深度(max_depth)。
如果欠拟合，尝试增加树深度或增加迭代次数。
输出格式要求：包含完整的 config 结构。
"""
        response = await self.async_call_llm("researcher", system_prompt, user_prompt)
        
        try:
            start = response.find("{")
            end = response.rfind("}")
            if start != -1 and end != -1:
                json_str = response[start:end+1]
                return json.loads(json_str)
        except Exception as e:
            logger.error(f"Failed to parse researcher JSON: {e}. Raw response: {response}")
        
        # 解析失败则返回原配置
        return current_config

    async def run_reviewer(self, draft_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        配置审查与构建员
        目前简化为结构校验，确保能发给 RDAgent。
        """
        # 确保关键字段存在
        if "model_params" not in draft_config:
            draft_config["model_params"] = {}
        if "factor_list" not in draft_config:
            draft_config["factor_list"] = []
        return draft_config
