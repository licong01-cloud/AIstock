"""
QuantEvolver: ExecutionAlgoAnalyst（执行算法分析师）

功能：
1. 规则引擎预分类（基于 algo_code 的确定性映射）
2. LLM 深度分析（分类、评分、适用场景、优劣势、A股注意事项）
3. 批量分析 SSE 流式推送
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.execution_analyst")

# 分类体系
CATEGORIES = {
    "SCHEDULE":   "定时调度型",
    "ADAPTIVE":   "自适应型",
    "PASSIVE":    "被动跟随型",
    "AGGRESSIVE": "激进执行型",
    "HYBRID":     "混合型",
}

# 6 维评分权重
SCORE_WEIGHTS = {
    "execution_quality":   0.25,
    "adaptiveness":        0.20,
    "data_feasibility":    0.15,
    "complexity_benefit":  0.15,
    "a_share_suitability": 0.15,
    "robustness":          0.10,
}

# 规则引擎分类映射
RULE_BASED_CATEGORIES = {
    "CLOSE_PRICE":     "SCHEDULE",
    "TWAP":            "SCHEDULE",
    "VWAP":            "PASSIVE",
    "POV":             "PASSIVE",
    "SBB_EMA":         "ADAPTIVE",
    "AC_OPTIMAL":      "ADAPTIVE",
    "TAIL_BOOST":      "ADAPTIVE",
    "TAIL_SUBSTITUTE": "ADAPTIVE",
}


def _score_to_grade(score: int) -> str:
    """综合评分 → 评级映射"""
    if score >= 90:
        return "S"
    if score >= 75:
        return "A"
    if score >= 60:
        return "B"
    if score >= 40:
        return "C"
    return "D"


def _weighted_score(scores: Dict[str, int]) -> int:
    """根据权重计算综合评分"""
    total = 0.0
    for dim, weight in SCORE_WEIGHTS.items():
        total += scores.get(dim, 50) * weight
    return round(total)


def _extract_json_from_response(content: str) -> Optional[Dict]:
    """从 LLM 响应中安全提取 JSON（处理 markdown 包裹）"""
    text = content.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # 去掉首行 ``` 和末行 ```
        start = 1
        end = len(lines)
        if lines[0].startswith("```"):
            start = 1
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end = i
                break
        text = "\n".join(lines[start:end])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # 尝试找到第一个 { 到最后一个 }
        first_brace = text.find("{")
        last_brace = text.rfind("}")
        if first_brace != -1 and last_brace != -1:
            try:
                return json.loads(text[first_brace:last_brace + 1])
            except json.JSONDecodeError:
                pass
    return None


class ExecutionAlgoAnalyst:
    """执行算法分析器 — 规则引擎 + LLM 双引擎"""

    def get_all_algorithms(self) -> List[Dict[str, Any]]:
        """获取所有执行算法"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, algo_code, algo_name, algo_type, description,
                           source, source_code, default_config, param_schema,
                           supported_freqs, min_bars,
                           category, category_reason, grade, grade_score,
                           analysis_profile, llm_analysis_at,
                           is_enabled, sort_order, created_at, updated_at
                    FROM execution_algorithm_catalog
                    ORDER BY sort_order, algo_code
                """)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                for row in rows:
                    for k in ("created_at", "updated_at", "llm_analysis_at"):
                        if row.get(k) and isinstance(row[k], datetime):
                            row[k] = row[k].isoformat()
                return rows

    def get_algorithm(self, algo_code: str) -> Optional[Dict[str, Any]]:
        """获取单个执行算法"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, algo_code, algo_name, algo_type, description,
                           source, source_code, default_config, param_schema,
                           supported_freqs, min_bars,
                           category, category_reason, grade, grade_score,
                           analysis_profile, llm_analysis_at,
                           is_enabled, sort_order, created_at, updated_at
                    FROM execution_algorithm_catalog
                    WHERE algo_code = %s
                """, (algo_code,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                data = dict(zip(cols, row))
                for k in ("created_at", "updated_at", "llm_analysis_at"):
                    if data.get(k) and isinstance(data[k], datetime):
                        data[k] = data[k].isoformat()
                return data

    def analyze_algorithm(self, algo_code: str, use_llm: bool = True) -> Dict[str, Any]:
        """分析单个执行算法"""
        algo = self.get_algorithm(algo_code)
        if not algo:
            return {"ok": False, "error": f"算法 {algo_code} 不存在"}

        # 1. 规则引擎预分类
        category = RULE_BASED_CATEGORIES.get(algo_code, "HYBRID")
        category_reason = f"规则引擎分类: {CATEGORIES.get(category, category)}"
        scores = None
        profile = None

        # 2. LLM 深度分析
        if use_llm:
            llm_result = self._analyze_with_llm(algo)
            if llm_result:
                category = llm_result.get("category", category)
                category_reason = llm_result.get("category_reason", category_reason)
                scores = llm_result.get("scores")
                profile = {
                    "applicable_scenarios": llm_result.get("applicable_scenarios", []),
                    "advantages": llm_result.get("advantages", []),
                    "disadvantages": llm_result.get("disadvantages", []),
                    "a_share_notes": llm_result.get("a_share_notes", ""),
                    "usage_guidance": llm_result.get("usage_guidance", ""),
                    "scores": scores,
                    "best_for": llm_result.get("best_for", []),
                    "avoid_for": llm_result.get("avoid_for", []),
                }

        # 3. 如果没有 LLM 结果，用规则引擎兜底生成评分
        if not scores:
            scores = self._rule_based_scores(algo_code)
            profile = {
                "applicable_scenarios": [],
                "advantages": [],
                "disadvantages": [],
                "a_share_notes": "",
                "usage_guidance": "",
                "scores": scores,
                "best_for": [],
                "avoid_for": [],
            }

        # 4. 计算综合评分和评级
        grade_score = _weighted_score(scores)
        grade = _score_to_grade(grade_score)

        # 5. 写回 DB
        self._update_analysis(
            algo_code=algo_code,
            category=category,
            category_reason=category_reason,
            grade=grade,
            grade_score=grade_score,
            analysis_profile=profile,
        )

        return {
            "ok": True,
            "algo_code": algo_code,
            "algo_name": algo.get("algo_name"),
            "category": category,
            "category_reason": category_reason,
            "grade": grade,
            "grade_score": grade_score,
            "analysis_profile": profile,
        }

    async def batch_analyze(self, use_llm: bool = True) -> AsyncGenerator[Dict[str, Any], None]:
        """批量分析所有算法（异步生成器，支持 SSE）"""
        import asyncio

        algos = self.get_all_algorithms()
        total = len(algos)
        analyzed = 0
        errors = []

        for algo in algos:
            try:
                result = await asyncio.to_thread(
                    self.analyze_algorithm,
                    algo_code=algo["algo_code"],
                    use_llm=use_llm,
                )
                if result.get("ok"):
                    analyzed += 1
                    yield {
                        "type": "progress",
                        "current": analyzed,
                        "total": total,
                        "algo_code": algo["algo_code"],
                        "algo_name": algo["algo_name"],
                        "grade": result.get("grade"),
                        "category": result.get("category"),
                    }
                else:
                    errors.append(f"{algo['algo_code']}: {result.get('error')}")
                    yield {"type": "error", "algo_code": algo["algo_code"], "error": result.get("error")}
            except Exception as e:
                errors.append(f"{algo['algo_code']}: {e}")
                yield {"type": "error", "algo_code": algo["algo_code"], "error": str(e)}

        yield {"type": "done", "analyzed": analyzed, "total": total, "errors": errors}

    def update_algorithm(self, algo_code: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """更新算法配置（启用/禁用/参数调整）"""
        set_parts = []
        params = []

        if "is_enabled" in updates:
            set_parts.append("is_enabled = %s")
            params.append(updates["is_enabled"])
        if "default_config" in updates:
            set_parts.append("default_config = %s")
            params.append(json.dumps(updates["default_config"]))
        if "description" in updates:
            set_parts.append("description = %s")
            params.append(updates["description"])
        if "sort_order" in updates:
            set_parts.append("sort_order = %s")
            params.append(updates["sort_order"])

        if not set_parts:
            return {"ok": False, "error": "无更新内容"}

        set_parts.append("updated_at = NOW()")
        params.append(algo_code)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE execution_algorithm_catalog SET {', '.join(set_parts)} WHERE algo_code = %s RETURNING id",
                    params,
                )
                row = cur.fetchone()
                if not row:
                    return {"ok": False, "error": f"算法 {algo_code} 不存在"}

        return {"ok": True, "algo_code": algo_code, "message": "更新成功"}

    # ---- 内部方法 ----

    def _analyze_with_llm(self, algo: Dict) -> Optional[Dict]:
        """使用 LLM 分析执行算法"""
        try:
            import litellm
        except ImportError:
            logger.warning("litellm 未安装，跳过 LLM 分析")
            return None

        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("execution_analyst", "analyze_execution_algo")

        if not prompt_data:
            logger.warning("execution_analyst prompt 未注册，跳过 LLM 分析")
            return None

        system_prompt = prompt_data["system_prompt"]
        user_prompt = safe_format(
            prompt_data["user_prompt_template"],
            algo_name=algo.get("algo_name", ""),
            algo_code=algo.get("algo_code", ""),
            description=algo.get("description", ""),
            source_code=algo.get("source_code", ""),
            default_config=json.dumps(algo.get("default_config", {}), ensure_ascii=False),
            supported_freqs=str(algo.get("supported_freqs", [])),
        )

        try:
            from .llm_client import get_llm_kwargs
            kwargs = get_llm_kwargs("execution_analyst")

            response = litellm.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
                max_tokens=1000,
                **kwargs,
            )
            content = response.choices[0].message.content.strip()
            result = _extract_json_from_response(content)
            if result:
                return result
            logger.warning(f"LLM 返回非 JSON: {content[:200]}")
        except Exception as e:
            logger.warning(f"LLM 执行算法分析失败 ({algo.get('algo_code')}): {e}")

        return None

    def _rule_based_scores(self, algo_code: str) -> Dict[str, int]:
        """规则引擎兜底评分"""
        presets = {
            "CLOSE_PRICE": {
                "execution_quality": 40, "adaptiveness": 5,
                "data_feasibility": 100, "complexity_benefit": 90,
                "a_share_suitability": 85, "robustness": 95,
            },
            "TWAP": {
                "execution_quality": 65, "adaptiveness": 10,
                "data_feasibility": 90, "complexity_benefit": 80,
                "a_share_suitability": 80, "robustness": 90,
            },
            "VWAP": {
                "execution_quality": 80, "adaptiveness": 30,
                "data_feasibility": 70, "complexity_benefit": 70,
                "a_share_suitability": 75, "robustness": 70,
            },
            "SBB_EMA": {
                "execution_quality": 70, "adaptiveness": 65,
                "data_feasibility": 85, "complexity_benefit": 60,
                "a_share_suitability": 70, "robustness": 60,
            },
            "AC_OPTIMAL": {
                "execution_quality": 85, "adaptiveness": 75,
                "data_feasibility": 65, "complexity_benefit": 55,
                "a_share_suitability": 60, "robustness": 55,
            },
            "POV": {
                "execution_quality": 75, "adaptiveness": 50,
                "data_feasibility": 50, "complexity_benefit": 65,
                "a_share_suitability": 65, "robustness": 65,
            },
        }
        return presets.get(algo_code, {
            "execution_quality": 50, "adaptiveness": 50,
            "data_feasibility": 50, "complexity_benefit": 50,
            "a_share_suitability": 50, "robustness": 50,
        })

    def _update_analysis(
        self,
        algo_code: str,
        category: str,
        category_reason: str,
        grade: str,
        grade_score: int,
        analysis_profile: Dict,
    ) -> None:
        """写回分析结果到 DB"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE execution_algorithm_catalog
                    SET category = %s,
                        category_reason = %s,
                        grade = %s,
                        grade_score = %s,
                        analysis_profile = %s,
                        llm_analysis_at = NOW(),
                        updated_at = NOW()
                    WHERE algo_code = %s
                """, (
                    category,
                    category_reason,
                    grade,
                    grade_score,
                    json.dumps(analysis_profile, ensure_ascii=False),
                    algo_code,
                ))
