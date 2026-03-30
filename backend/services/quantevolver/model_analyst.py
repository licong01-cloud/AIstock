"""
QuantEvolver: ModelAnalyst（模型分析师）

功能：
1. 分析单个模型：基于模型架构、假设文本、超参数、训练诊断等生成双输出
   - description: 人类可读文本描述（100-300字）
   - analysis_profile: JSON 结构化数据（供 QE 演进 Agent 程序化读取）
2. 批量分析所有模型
3. 批量分析SSE流式推送
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.model_analyst")


# 模型等级定义
MODEL_GRADES = {
    "S": "卓越 — 各维度表现突出，训练收敛良好",
    "A": "优秀 — 多数维度表现优秀",
    "B": "良好 — 表现合格但有提升空间",
    "C": "一般 — 存在明显短板",
    "D": "较差 — 多维度表现不佳",
}


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

    # 训练诊断摘要
    best_epoch = model_info.get("best_epoch")
    total_epochs = model_info.get("total_epochs")
    convergence_ratio = model_info.get("convergence_ratio")
    overfit_ratio = model_info.get("overfit_ratio")
    if best_epoch is not None and total_epochs:
        parts.append(f"。训练：best_epoch={best_epoch}/{total_epochs}")
        if convergence_ratio is not None:
            parts.append(f", 收敛率={convergence_ratio:.2f}")
        if overfit_ratio is not None:
            parts.append(f", 过拟合率={overfit_ratio:.2f}")

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


def _compute_model_grade_by_rules(model_info: Dict) -> tuple:
    """基于规则计算模型等级（S/A/B/C/D）和训练质量评分（0-100）。

    Returns:
        (grade, grade_reason, training_quality_score)
    """
    ic = model_info.get("ic")
    ann_ret = model_info.get("annualized_return")
    max_dd = model_info.get("max_drawdown")
    ir = model_info.get("information_ratio")
    best_epoch = model_info.get("best_epoch")
    total_epochs = model_info.get("total_epochs")
    convergence_ratio = model_info.get("convergence_ratio")
    overfit_ratio = model_info.get("overfit_ratio")
    training_failed = model_info.get("training_failed", False)

    score = 50  # 基础分
    reasons = []

    if training_failed:
        return "D", "训练失败", 0

    # 回测表现评分 (最多 +30)
    if ic is not None:
        if ic >= 0.06:
            score += 15
        elif ic >= 0.04:
            score += 10
        elif ic >= 0.02:
            score += 5
        else:
            score -= 5

    if ir is not None:
        if ir >= 2.0:
            score += 15
        elif ir >= 1.0:
            score += 10
        elif ir >= 0.5:
            score += 5
        else:
            score -= 5

    # 训练质量评分 (最多 +20)
    if convergence_ratio is not None:
        if convergence_ratio >= 0.8:
            score += 10
        elif convergence_ratio >= 0.5:
            score += 5
        elif convergence_ratio < 0.2:
            score -= 10
            reasons.append("收敛不足")

    if overfit_ratio is not None:
        if overfit_ratio <= 0.1:
            score += 10
        elif overfit_ratio <= 0.3:
            score += 5
        elif overfit_ratio > 0.5:
            score -= 10
            reasons.append("过拟合严重")

    if best_epoch is not None and total_epochs:
        if best_epoch == 0:
            score -= 15
            reasons.append("模型未学习(best_epoch=0)")
        elif best_epoch / total_epochs < 0.1:
            score -= 5
            reasons.append("过早停止")

    score = max(0, min(100, score))

    # 映射到等级
    if score >= 85:
        grade = "S"
    elif score >= 70:
        grade = "A"
    elif score >= 55:
        grade = "B"
    elif score >= 40:
        grade = "C"
    else:
        grade = "D"

    if not reasons:
        reasons.append(MODEL_GRADES.get(grade, ""))

    return grade, "；".join(reasons), score


def _build_analysis_profile(model_info: Dict, grade: str, grade_reason: str, tq_score: float) -> Dict:
    """构建模型分析 JSON profile（供 QE 演进 Agent 读取）。"""
    return {
        "model_id": model_info.get("model_id"),
        "model_type": model_info.get("model_type"),
        "model_grade": grade,
        "grade_reason": grade_reason,
        "training_quality_score": tq_score,
        # 回测指标
        "ic": model_info.get("ic"),
        "annualized_return": model_info.get("annualized_return"),
        "max_drawdown": model_info.get("max_drawdown"),
        "information_ratio": model_info.get("information_ratio"),
        # 训练诊断
        "best_epoch": model_info.get("best_epoch"),
        "total_epochs": model_info.get("total_epochs"),
        "convergence_ratio": model_info.get("convergence_ratio"),
        "overfit_ratio": model_info.get("overfit_ratio"),
        "training_failed": model_info.get("training_failed", False),
        "train_loss_final": model_info.get("train_loss_final"),
        "val_loss_final": model_info.get("val_loss_final"),
        # 元数据
        "is_sota": model_info.get("is_sota"),
        "task_run_id": model_info.get("task_run_id"),
        "loop_id": model_info.get("loop_id"),
    }


def _generate_model_analysis_with_llm(model_info: Dict) -> Optional[Dict]:
    """使用 LLM 生成模型分析双输出（description + grade + profile）。

    Returns:
        {"description": str, "grade": str, "grade_reason": str,
         "training_quality_score": float, "profile": dict} or None
    """
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

    if not prompt_data:
        return None

    system_prompt = prompt_data["system_prompt"]

    # 构建增强的 user prompt（包含训练诊断数据）
    training_section = ""
    if model_info.get("best_epoch") is not None:
        training_section = (
            f"\n训练诊断：best_epoch={model_info.get('best_epoch')}/{model_info.get('total_epochs', '?')}, "
            f"收敛率={model_info.get('convergence_ratio', 'N/A')}, "
            f"过拟合率={model_info.get('overfit_ratio', 'N/A')}, "
            f"train_loss={model_info.get('train_loss_final', 'N/A')}, "
            f"val_loss={model_info.get('val_loss_final', 'N/A')}"
        )

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
    # 追加训练诊断信息到 prompt 末尾
    if training_section:
        user_prompt += training_section

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
        return {
            "description": (result.get("description") or "")[:300],
            "grade": result.get("grade"),
            "grade_reason": result.get("grade_reason"),
            "training_quality_score": result.get("training_quality_score"),
        }
    except Exception as e:
        logger.warning(f"LLM模型分析失败 ({model_name}): {e}")
        return None


class ModelAnalyst:
    """模型分析师Agent — 双输出（文本 description + JSON analysis_profile）。"""

    def analyze_single_model(
        self,
        model_id: str,
        use_llm: bool = False,
    ) -> Dict[str, Any]:
        """分析单个模型，生成双输出。

        Returns:
            {
                "ok": True,
                "model_id": str,
                "model_name": str,
                "description": str,          # 人类可读文本
                "model_grade": str,           # S/A/B/C/D
                "grade_reason": str,
                "training_quality_score": float,  # 0-100
                "analysis_profile": dict,     # 完整 JSON（供 Agent 读取）
            }
        """
        model_info = self._get_model_info(model_id)
        if not model_info:
            return {"ok": False, "error": f"模型 {model_id} 不存在"}

        # 尝试 LLM 生成
        description = None
        grade = None
        grade_reason = None
        tq_score = None

        if use_llm:
            llm_result = _generate_model_analysis_with_llm(model_info)
            if llm_result:
                description = llm_result.get("description")
                grade = llm_result.get("grade")
                grade_reason = llm_result.get("grade_reason")
                tq_score = llm_result.get("training_quality_score")

        # 规则兜底
        if not description:
            description = _generate_model_description_by_rules(model_info)
        if not grade:
            grade, grade_reason, tq_score = _compute_model_grade_by_rules(model_info)

        # 构建 analysis_profile JSON
        profile = _build_analysis_profile(model_info, grade, grade_reason, tq_score)

        # 保存双输出到数据库
        self._update_model_analysis(model_id, description, grade, grade_reason, tq_score, profile)

        return {
            "ok": True,
            "model_id": model_id,
            "model_name": model_info.get("model_name"),
            "description": description,
            "model_grade": grade,
            "grade_reason": grade_reason,
            "training_quality_score": tq_score,
            "analysis_profile": profile,
        }

    def batch_analyze_all_models(
        self,
        use_llm: bool = False,
        model_ids: list = None,
    ) -> Dict[str, Any]:
        """批量分析模型。model_ids 为空则分析全部。"""
        if model_ids:
            models = [{"model_id": mid} for mid in model_ids]
        else:
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

    async def batch_analyze_all_models_async(
        self,
        use_llm: bool = False,
        model_ids: list = None,
    ):
        """批量分析模型（异步生成器，支持SSE流式推送）。

        Yields:
            进度事件字典: {"type": "progress", "current": int, "total": int, "model_name": str, "grade": str}
            或错误事件: {"type": "error", "model_name": str, "error": str}
            或完成事件: {"type": "done", "analyzed": int, "total": int, "errors": list}
        """
        import asyncio

        if model_ids:
            models = [{"model_id": mid} for mid in model_ids]
        else:
            models = self._get_all_models()
        total = len(models)
        analyzed = 0
        errors = []
        semaphore = asyncio.Semaphore(3)

        async def analyze_one(m, index):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        self.analyze_single_model,
                        model_id=m["model_id"],
                        use_llm=use_llm,
                    )
                    return {
                        "ok": True,
                        "model_id": m["model_id"],
                        "model_name": result.get("model_name", m["model_id"]),
                        "grade": result.get("model_grade"),
                        "index": index,
                    }
                except Exception as e:
                    return {"ok": False, "model_id": m["model_id"], "model_name": m["model_id"], "error": str(e), "index": index}

        tasks = [analyze_one(m, i) for i, m in enumerate(models, 1)]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result["ok"]:
                analyzed += 1
                yield {
                    "type": "progress",
                    "current": analyzed,
                    "total": total,
                    "model_name": result["model_name"],
                    "grade": result.get("grade"),
                }
            else:
                errors.append(f"{result['model_id']}: {result['error']}")
                yield {"type": "error", "model_name": result["model_name"], "error": result["error"]}

        yield {"type": "done", "analyzed": analyzed, "total": total, "errors": errors}

    # ---- 内部方法 ----

    def _get_model_info(self, model_id: str) -> Optional[Dict]:
        """获取模型详细信息（含训练诊断字段）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id, model_name, model_type, display_name,
                           ic, annualized_return, max_drawdown, information_ratio,
                           is_sota, task_run_id, loop_id,
                           hypothesis_text, model_architecture,
                           model_hyperparameters, model_training_hyperparameters,
                           generated_at_utc,
                           best_epoch, total_epochs, convergence_ratio, overfit_ratio,
                           training_failed, train_loss_final, val_loss_final
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

    def _update_model_analysis(
        self,
        model_id: str,
        description: str,
        grade: str,
        grade_reason: str,
        tq_score: float,
        profile: Dict,
    ) -> None:
        """更新模型分析双输出到数据库。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE aistock_model_catalog
                    SET description = %s,
                        model_grade = %s,
                        grade_reason = %s,
                        training_quality_score = %s,
                        analysis_profile = %s
                    WHERE model_id = %s
                """, (
                    description,
                    grade,
                    grade_reason,
                    tq_score,
                    json.dumps(profile, ensure_ascii=False, default=str),
                    model_id,
                ))
