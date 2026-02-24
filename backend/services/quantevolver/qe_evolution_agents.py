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
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_analyst", "diagnose_experiment")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = safe_format(prompt_data["user_prompt_template"], 
                loop_index=loop_index,
                config=json.dumps(config, ensure_ascii=False),
                metrics=json.dumps(metrics, ensure_ascii=False),
                analysis_context=json.dumps(analysis_context or {}, ensure_ascii=False),
            )
        else:
            raise ValueError("未配置 evolution_analyst/diagnose_experiment 的提示词，拒绝使用兜底策略")
            
        return await self.async_call_llm("evolution_analyst", system_prompt, user_prompt)

    async def run_evaluator(self, current_metrics: Dict[str, Any], historical_sota_metrics: Optional[Dict[str, Any]]) -> bool:
        """
        SOTA 评估官
        """
        if not historical_sota_metrics:
            return True # 第一轮默认是 SOTA

        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_evaluator", "evaluate_sota")

        if prompt_data:
            try:
                system_prompt = prompt_data["system_prompt"]
                user_prompt = safe_format(prompt_data["user_prompt_template"], 
                    current_metrics=json.dumps(current_metrics, ensure_ascii=False),
                    historical_sota_metrics=json.dumps(historical_sota_metrics, ensure_ascii=False),
                )
                response = await self.async_call_llm("evolution_evaluator", system_prompt, user_prompt)

                start = response.find("{")
                end = response.rfind("}")
                if start != -1 and end != -1:
                    payload = json.loads(response[start:end+1])
                    if "is_sota" in payload:
                        return bool(payload.get("is_sota"))
            except Exception as e:
                logger.warning(f"evolution_evaluator提示词调用或解析失败: {e}")
        else:
            logger.warning("未配置 evolution_evaluator/evaluate_sota 提示词")
            
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
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_researcher", "propose_config")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = safe_format(prompt_data["user_prompt_template"], 
                analyst_report=analyst_report,
                sota_status=sota_status,
                current_config=json.dumps(current_config, ensure_ascii=False)
            )
        else:
            raise ValueError("未配置 evolution_researcher/propose_config 的提示词，拒绝使用兜底策略")
            
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
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_reviewer", "review_config")

        if prompt_data:
            try:
                system_prompt = prompt_data["system_prompt"]
                user_prompt = safe_format(prompt_data["user_prompt_template"], 
                    draft_config=json.dumps(draft_config, ensure_ascii=False)
                )
                response = await self.async_call_llm("evolution_reviewer", system_prompt, user_prompt)

                start = response.find("{")
                end = response.rfind("}")
                if start != -1 and end != -1:
                    payload = json.loads(response[start:end+1])
                    approved = bool(payload.get("approved", False))
                    if not approved:
                        raise ValueError(payload.get("reason", "Reviewer rejected config"))
                    candidate = payload.get("config")
                    if isinstance(candidate, dict):
                        draft_config = candidate
            except Exception as e:
                logger.warning(f"evolution_reviewer提示词调用或解析失败: {e}")
        else:
            logger.warning("未配置 evolution_reviewer/review_config 提示词")

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
