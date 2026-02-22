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
            raise RuntimeError("litellm is not installed, evolution agents cannot run")
            
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
            raise
            
    async def async_call_llm(self, agent_type: str, system_prompt: str, user_prompt: str) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._call_llm, agent_type, system_prompt, user_prompt)
        
    async def run_analyst(
        self,
        loop_index: int,
        config: Dict[str, Any],
        metrics: Dict[str, Any],
        analysis_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """实验诊断分析师"""
        from .prompt_manager import PromptManager
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_analyst", "diagnose_experiment")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = prompt_data["user_prompt_template"].format(
                loop_index=loop_index,
                config=json.dumps(config, ensure_ascii=False),
                metrics=json.dumps(metrics, ensure_ascii=False),
                analysis_context=json.dumps(analysis_context or {}, ensure_ascii=False),
            )
        else:
            system_prompt = "你是一位量化实验诊断分析师(Analyst)。你的任务是根据量化模型回测的配置和结果指标，诊断当前组合的瓶颈和表现。请给出简短、专业的结论。"
            user_prompt = f"""
请分析以下实验结果：
- Loop 轮次: {loop_index}
- 实验配置: {json.dumps(config, ensure_ascii=False)}
- 回测核心指标: {json.dumps(metrics, ensure_ascii=False)}
- 复用历史多维分析上下文: {json.dumps(analysis_context or {}, ensure_ascii=False)}

请给出一段简明扼要的诊断报告（100字左右），指出表现优异的地方以及可能存在的过拟合或特征衰减风险。
"""
        return await self.async_call_llm("evolution_analyst", system_prompt, user_prompt)

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
        
        # SOTA 规则：IC 提升 或者 IC 差距在 0.005 以内且 ICIR 显著提升
        if cur_ic > sota_ic + 0.002:
            return True
        if abs(cur_ic - sota_ic) <= 0.005 and cur_icir > sota_icir + 0.05:
            return True
            
        return False

    async def run_researcher(self, analyst_report: str, sota_status: bool, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        演进策略研究员
        """
        from .prompt_manager import PromptManager
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_researcher", "propose_config")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = prompt_data["user_prompt_template"].format(
                analyst_report=analyst_report,
                sota_status=sota_status,
                current_config=json.dumps(current_config, ensure_ascii=False)
            )
        else:
            system_prompt = "你是一位高级量化策略研究员(Researcher)。你的任务是根据上一轮的诊断报告，决策下一轮的配置方案（决定是调整因子、调整超参，还是更换模型）。只输出 JSON 格式的更新配置，不要输出其他废话。"
            user_prompt = f"""
上一轮诊断意见: {analyst_report}
上一轮是否达到 SOTA (最优): {sota_status}
当前基础配置: {json.dumps(current_config, ensure_ascii=False)}

请给出下一轮建议的实验配置(格式必须为严格的 JSON)。
如果你决定调整超参（如过拟合则增加正则化或减少树深度，欠拟合则增加树深度或迭代次数），action_type 为 "param_tune"。
如果你决定删减或新增特征因子，action_type 为 "factor_adjust"。
如果你决定更换模型结构，action_type 为 "model_switch"。
输出格式要求：包含完整的 config 结构，必须包含 "action_type" 字段。
"""
        response = await self.async_call_llm("evolution_researcher", system_prompt, user_prompt)
        
        try:
            start = response.find("{")
            end = response.rfind("}")
            if start != -1 and end != -1:
                json_str = response[start:end+1]
                return json.loads(json_str)
        except Exception as e:
            logger.error(f"Failed to parse researcher JSON: {e}. Raw response: {response}")
        
        raise ValueError(f"Researcher output is not valid JSON config: {response}")

    async def run_reviewer(self, draft_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        配置审查与构建员
        目前简化为结构校验，确保能发给 RDAgent。
        """
        required_keys = ["model_params", "factor_list", "action_type"]
        missing = [k for k in required_keys if k not in draft_config]
        if missing:
            raise ValueError(f"Reviewer rejected config, missing required keys: {missing}")

        if not isinstance(draft_config["model_params"], dict):
            raise ValueError("Reviewer rejected config: model_params must be an object")
        if not isinstance(draft_config["factor_list"], list):
            raise ValueError("Reviewer rejected config: factor_list must be an array")

        action_type = draft_config["action_type"]
        allowed_action_types = {"initial", "factor_adjust", "param_tune", "model_switch"}
        if not isinstance(action_type, str) or action_type not in allowed_action_types:
            raise ValueError(
                f"Reviewer rejected config: action_type must be one of {sorted(allowed_action_types)}"
            )

        return draft_config
