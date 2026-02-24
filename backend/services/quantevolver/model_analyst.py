"""
QuantEvolver: ModelAnalyst（模型分析师）

功能：
1. 分析单个模型：基于模型架构、假设文本、超参数等生成100-200字描述
2. 批量分析所有模型
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.model_analyst")


def _generate_model_description_by_rules(model_info: Dict) -> str:
    """基于规则生成模型描述（100-200字）。"""
    model_name = model_info.get("model_name", "")
    model_type = (model_info.get("model_type") or "").upper()
    hypothesis = model_info.get("hypothesis_text") or ""
    architecture = model_info.get("model_architecture") or ""
    hp = model_info.get("model_hyperparameters") or ""
    ic = model_info.get("ic")
    ann_ret = model_info.get("annualized_return")
    max_dd = model_info.get("max_drawdown")
    ir = model_info.get("information_ratio")

    parts = []

    # 模型类型描述
    if "LGB" in model_type or "LGBM" in model_type or "GBDT" in model_type:
        parts.append("基于LightGBM梯度提升树的量化预测模型")
    elif "LSTM" in model_type:
        parts.append("基于LSTM长短期记忆网络的时序预测模型")
    elif "TRANSFORMER" in model_type or "ATTENTION" in model_type:
        parts.append("基于Transformer注意力机制的量化预测模型")
    elif "NN" in model_type or "MLP" in model_type or "DNN" in model_type:
        parts.append("基于深度神经网络的量化预测模型")
    elif "LINEAR" in model_type:
        parts.append("基于线性回归的量化预测模型")
    elif "XGB" in model_type:
        parts.append("基于XGBoost梯度提升的量化预测模型")
    elif "CATBOOST" in model_type:
        parts.append("基于CatBoost的量化预测模型")
    else:
        parts.append(f"{model_type or '未知类型'}量化预测模型")

    # 从假设文本提取关键信息
    if hypothesis:
        hyp_short = hypothesis[:80].strip()
        if len(hypothesis) > 80:
            hyp_short += "..."
        parts.append(f"。研究假设：{hyp_short}")

    # 从架构信息提取
    if architecture and not hypothesis:
        arch_short = architecture[:60].strip()
        if len(architecture) > 60:
            arch_short += "..."
        parts.append(f"。架构：{arch_short}")

    # 超参数摘要
    if hp:
        if isinstance(hp, str):
            try:
                hp_dict = json.loads(hp)
            except Exception:
                hp_dict = {}
        else:
            hp_dict = hp
        if isinstance(hp_dict, dict):
            key_params = []
            if "num_leaves" in hp_dict:
                key_params.append(f"叶子数={hp_dict['num_leaves']}")
            if "max_depth" in hp_dict:
                key_params.append(f"深度={hp_dict['max_depth']}")
            if "learning_rate" in hp_dict:
                key_params.append(f"学习率={hp_dict['learning_rate']}")
            if "d_model" in hp_dict:
                key_params.append(f"d_model={hp_dict['d_model']}")
            if "nhead" in hp_dict:
                key_params.append(f"注意力头={hp_dict['nhead']}")
            if key_params:
                parts.append(f"。关键超参：{', '.join(key_params[:3])}")

    # 性能指标
    perf_parts = []
    if ic is not None:
        perf_parts.append(f"IC={ic:.4f}")
    if ann_ret is not None:
        perf_parts.append(f"年化收益={ann_ret*100:.1f}%")
    if max_dd is not None:
        perf_parts.append(f"最大回撤={max_dd*100:.1f}%")
    if ir is not None:
        perf_parts.append(f"IR={ir:.3f}")
    if perf_parts:
        parts.append(f"。性能：{', '.join(perf_parts)}")

    # 组合搭配建议
    if "LGB" in model_type or "LGBM" in model_type or "GBDT" in model_type or "XGB" in model_type:
        parts.append("。适合20-80个因子的中等规模因子集，擅长捕捉非线性交互信号，推荐搭配多类别因子组合")
    elif "LSTM" in model_type or "TRANSFORMER" in model_type or "ATTENTION" in model_type:
        parts.append("。适合大规模因子集(50+)，擅长捕捉时序模式，推荐搭配动量和技术类因子")
    elif "NN" in model_type or "MLP" in model_type or "DNN" in model_type:
        parts.append("。适合中大规模因子集(30-100)，擅长捕捉非线性关系，推荐搭配多样化因子组合")
    elif "LINEAR" in model_type:
        parts.append("。适合精选少量高质量因子(10-30)，擅长捕捉线性关系，推荐搭配低相关性因子")

    desc = "".join(parts)
    return desc[:300]


def _generate_model_description_with_llm(model_info: Dict) -> Optional[str]:
    """使用LLM生成模型描述。"""
    try:
        import litellm
    except ImportError:
        return None

    model_name = model_info.get("model_name", "")
    model_type = model_info.get("model_type", "")
    hypothesis = model_info.get("hypothesis_text") or ""
    architecture = model_info.get("model_architecture") or ""
    hp = model_info.get("model_hyperparameters") or ""

    from .prompt_manager import PromptManager, safe_format
    pm = PromptManager()
    prompt_data = pm.get_active_prompt_text("model_analyst", "generate_description")

    if prompt_data:
        system_prompt = prompt_data["system_prompt"]
        user_prompt = safe_format(prompt_data["user_prompt_template"], 
            model_name=model_name,
            model_type=model_type,
            hypothesis=(hypothesis[:500] if hypothesis else "无"),
            architecture=(architecture[:300] if architecture else "无"),
            hyperparameters=(str(hp)[:300] if hp else "无"),
            ic=model_info.get("ic"),
            annualized_return=model_info.get("annualized_return"),
            max_drawdown=model_info.get("max_drawdown"),
            information_ratio=model_info.get("information_ratio"),
        )
    else:
        raise ValueError("未配置 model_analyst/generate_description 的提示词，拒绝使用兜底策略")

    try:
        from .llm_client import get_llm_kwargs
        kwargs = get_llm_kwargs("model_analyst")
        
        response = litellm.completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=400,
            **kwargs
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        result = json.loads(content)
        return result.get("description", "")[:200]
    except Exception as e:
        logger.warning(f"LLM模型描述生成失败 ({model_name}): {e}")
        return None


class ModelAnalyst:
    """模型分析师Agent。"""

    def analyze_single_model(
        self,
        model_id: str,
        use_llm: bool = False,
    ) -> Dict[str, Any]:
        """分析单个模型，生成描述。"""
        model_info = self._get_model_info(model_id)
        if not model_info:
            return {"ok": False, "error": f"模型 {model_id} 不存在"}

        # 生成描述
        description = None
        if use_llm:
            description = _generate_model_description_with_llm(model_info)
        if not description:
            description = _generate_model_description_by_rules(model_info)

        # 保存到数据库
        self._update_model_description(model_id, description)

        return {
            "ok": True,
            "model_id": model_id,
            "model_name": model_info.get("model_name"),
            "description": description,
        }

    def batch_analyze_all_models(
        self,
        use_llm: bool = False,
    ) -> Dict[str, Any]:
        """批量分析所有模型。"""
        models = self._get_all_models()
        total = len(models)
        analyzed = 0
        errors = []

        for m in models:
            try:
                self.analyze_single_model(
                    model_id=m["model_id"],
                    use_llm=use_llm,
                )
                analyzed += 1
            except Exception as e:
                errors.append(f"{m['model_id']}: {e}")

        return {
            "ok": len(errors) == 0,
            "total": total,
            "analyzed": analyzed,
            "errors": errors,
        }

    # ---- 内部方法 ----

    def _get_model_info(self, model_id: str) -> Optional[Dict]:
        """获取模型详细信息。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id, model_name, model_type, display_name,
                           ic, annualized_return, max_drawdown, information_ratio,
                           is_sota, task_run_id, loop_id,
                           hypothesis_text, model_architecture,
                           model_hyperparameters, model_training_hyperparameters,
                           generated_at_utc
                    FROM aistock_model_catalog
                    WHERE model_id = %s
                """, (model_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))

    def _get_all_models(self):
        """获取所有模型列表。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT model_id FROM aistock_model_catalog")
                return [{"model_id": row[0]} for row in cur.fetchall()]

    def _update_model_description(self, model_id: str, description: str) -> None:
        """更新模型描述到数据库。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE aistock_model_catalog
                    SET description = %s
                    WHERE model_id = %s
                """, (description, model_id))
