"""
QuantEvolver Phase 2: PortfolioArchitect（组合架构师）

功能：
1. 配置评估：用户选择因子+模型+策略后给出分析和建议
2. 智能推荐：推荐多种不同特征的组合
3. 风险提示：识别因子重叠、过拟合风险、策略不匹配等
4. 智能生成：基于用户自然语言需求+因子库/模型库元数据自动生成最佳组合
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.portfolio_architect")


class PortfolioArchitect:
    """组合架构师Agent。"""

    def evaluate_combination(
        self,
        factor_names: List[str],
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        custom_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """评估因子+模型+策略组合（使用LLM进行真实分析）。

        分析维度：
        1. 因子覆盖度：类别分布是否均衡
        2. 因子质量：平均评级
        3. 因子冗余：同类别因子是否过多
        4. 模型适配性：模型类型与因子特征是否匹配
        5. 策略合理性：策略参数是否在合理范围
        6. LLM综合分析：结合因子+模型+策略给出完整评论
        """
        analysis = {
            "ok": True,
            "factor_analysis": {},
            "model_analysis": {},
            "strategy_analysis": {},
            "risks": [],
            "suggestions": [],
            "overall_score": 0.0,
            "llm_commentary": "",
        }

        # 1. 因子分析
        factor_analysis = self._analyze_factors(factor_names)
        analysis["factor_analysis"] = factor_analysis

        # 2. 模型分析
        if model_id:
            model_analysis = self._analyze_model(model_id, factor_names)
            analysis["model_analysis"] = model_analysis

        # 3. 策略分析
        if strategy_id:
            strategy_analysis = self._analyze_strategy(strategy_id, custom_params)
            analysis["strategy_analysis"] = strategy_analysis

        # 4. 风险识别（规则基础）
        risks = self._identify_risks(factor_analysis, analysis.get("model_analysis"), analysis.get("strategy_analysis"))
        analysis["risks"] = risks

        # 5. 建议生成（规则基础）
        suggestions = self._generate_suggestions(factor_analysis, analysis.get("model_analysis"), risks)
        analysis["suggestions"] = suggestions

        # 6. 综合评分（规则基础）
        analysis["overall_score"] = self._compute_overall_score(factor_analysis, risks)

        # 7. LLM综合分析评论（失败不影响规则分析结果）
        llm_result = None
        try:
            llm_result = self._llm_evaluate_combination(
                factor_analysis=factor_analysis,
                model_analysis=analysis.get("model_analysis", {}),
                strategy_analysis=analysis.get("strategy_analysis", {}),
                factor_names=factor_names,
                custom_params=custom_params,
            )
        except Exception as e:
            logger.warning(f"LLM综合评估异常，规则分析结果不受影响: {e}")
        if llm_result:
            analysis["llm_commentary"] = llm_result.get("commentary", "")
            # LLM可以补充风险和建议
            if llm_result.get("additional_risks"):
                for r in llm_result["additional_risks"]:
                    analysis["risks"].append({"level": "info", "type": "llm_insight", "message": r})
            if llm_result.get("additional_suggestions"):
                analysis["suggestions"].extend(llm_result["additional_suggestions"])
            # LLM评分可以微调综合评分
            if llm_result.get("llm_score") is not None:
                rule_score = analysis["overall_score"]
                llm_score = llm_result["llm_score"]
                # 加权平均：规则40% + LLM60%
                analysis["overall_score"] = round(rule_score * 0.4 + llm_score * 0.6, 1)

        return analysis

    def _llm_evaluate_combination(
        self,
        factor_analysis: Dict,
        model_analysis: Dict,
        strategy_analysis: Dict,
        factor_names: List[str],
        custom_params: Optional[Dict] = None,
    ) -> Optional[Dict[str, Any]]:
        """使用LLM对组合进行综合分析评论。"""
        try:
            import litellm
        except ImportError:
            logger.warning("litellm未安装，跳过LLM评估")
            return None

        from .factor_analyst import FACTOR_CATEGORIES

        # 构建因子摘要
        categories = factor_analysis.get("categories", {})
        factor_summary_parts = []
        for cat, info in categories.items():
            cat_name = FACTOR_CATEGORIES.get(cat, cat)
            factor_summary_parts.append(f"{cat_name}({cat}): {info['count']}个因子 - {', '.join(info.get('factors', [])[:5])}")
        factor_summary = "\n".join(factor_summary_parts) if factor_summary_parts else "无分类信息"

        # 构建模型摘要
        model_summary = "未选择模型"
        if model_analysis and model_analysis.get("found"):
            m = model_analysis
            model_summary = (
                f"模型: {m.get('model_name', 'N/A')} (类型: {m.get('model_type', 'N/A')})\n"
                f"IC: {m.get('ic', 'N/A')}, 年化收益: {m.get('annualized_return', 'N/A')}, "
                f"最大回撤: {m.get('max_drawdown', 'N/A')}, SOTA: {'是' if m.get('is_sota') else '否'}\n"
                f"适配性: {'; '.join(m.get('compatibility_notes', []))}"
            )

        # 构建策略摘要
        strategy_summary = "未选择策略"
        if strategy_analysis and strategy_analysis.get("found"):
            s = strategy_analysis
            params = s.get("effective_params", {})
            strategy_summary = (
                f"策略: {s.get('display_name', 'N/A')}\n"
                f"参数: topk={params.get('topk', 'N/A')}, n_drop={params.get('n_drop', 'N/A')}\n"
                f"警告: {'; '.join(s.get('warnings', [])) or '无'}"
            )

        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("portfolio_architect", "evaluate_combination")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = safe_format(prompt_data["user_prompt_template"], 
                factor_count=factor_analysis.get('count', 0),
                classified_count=factor_analysis.get('classified_count', 0),
                category_count=factor_analysis.get('category_count', 0),
                avg_grade_score=factor_analysis.get('avg_grade_score', 0),
                diversity_score=factor_analysis.get('diversity_score', 0),
                factor_summary=factor_summary,
                model_summary=model_summary,
                strategy_summary=strategy_summary
            )
        else:
            raise ValueError("未配置 portfolio_architect/evaluate_combination 的提示词，拒绝使用兜底策略")

        try:
            from .llm_client import get_llm_kwargs
            kwargs = get_llm_kwargs("portfolio_architect")
            
            response = litellm.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=1500,
                **kwargs
            )
            content = response.choices[0].message.content.strip()
            # 解析JSON
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            result = json.loads(content)
            logger.info(f"LLM评估完成: score={result.get('llm_score')}")
            return result
        except Exception as e:
            logger.warning(f"LLM评估失败: {e}")
            return None

    def recommend_combinations(
        self,
        target_profiles: Optional[List[str]] = None,
        max_combinations: int = 3,
    ) -> Dict[str, Any]:
        """推荐多种不同特征的组合。

        target_profiles: 目标特征列表
            - "high_return": 高收益导向
            - "low_drawdown": 低回撤导向
            - "high_sharpe": 高夏普导向
            - "balanced": 均衡配置
        """
        if not target_profiles:
            target_profiles = ["high_return", "low_drawdown", "balanced"]

        combinations = []

        for profile in target_profiles[:max_combinations]:
            combo = self._build_combination_for_profile(profile)
            if combo:
                combinations.append(combo)

        return {
            "ok": True,
            "combinations": combinations,
            "profiles_requested": target_profiles,
        }

    # ---- 内部分析方法 ----

    def _analyze_factors(self, factor_names: List[str]) -> Dict[str, Any]:
        """分析因子组合特征。"""
        if not factor_names:
            return {"count": 0, "categories": {}, "avg_grade_score": 0}

        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT factor_name, category, grade, ic_value, sharpe_value, ann_ret_value
                    FROM qe_factor_classification
                    WHERE factor_name IN ({placeholders})
                """, factor_names)
                classified = {row[0]: {
                    "category": row[1], "grade": row[2],
                    "ic": row[3], "sharpe": row[4], "ann_ret": row[5],
                } for row in cur.fetchall()}

        # 类别分布
        categories = {}
        grade_scores = {"S": 5, "A": 4, "B": 3, "C": 2, "D": 1}
        total_grade_score = 0
        classified_count = 0

        for fn in factor_names:
            info = classified.get(fn, {})
            cat = info.get("category", "UNKNOWN")
            if cat not in categories:
                categories[cat] = {"count": 0, "factors": []}
            categories[cat]["count"] += 1
            categories[cat]["factors"].append(fn)

            grade = info.get("grade")
            if grade:
                total_grade_score += grade_scores.get(grade, 0)
                classified_count += 1

        avg_grade_score = total_grade_score / classified_count if classified_count > 0 else 0

        # 多样性评分（类别数/总因子数的比值）
        diversity = len(categories) / max(len(factor_names), 1)

        return {
            "count": len(factor_names),
            "classified_count": classified_count,
            "categories": categories,
            "category_count": len(categories),
            "avg_grade_score": round(avg_grade_score, 2),
            "diversity_score": round(diversity, 3),
        }

    def _analyze_model(self, model_id: str, factor_names: List[str]) -> Dict[str, Any]:
        """分析模型适配性。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id, model_name, model_type, ic, annualized_return,
                           max_drawdown, information_ratio, is_sota
                    FROM aistock_model_catalog
                    WHERE model_id = %s
                """, (model_id,))
                row = cur.fetchone()

        if not row:
            return {"found": False, "model_id": model_id}

        cols = ["model_id", "model_name", "model_type", "ic", "annualized_return",
                "max_drawdown", "information_ratio", "is_sota"]
        model_info = dict(zip(cols, row))

        # 模型类型与因子数量的适配性分析
        factor_count = len(factor_names)
        model_type = model_info.get("model_type", "")
        compatibility_notes = []

        if "LGB" in model_type.upper() or "LGBM" in model_type.upper():
            if factor_count > 100:
                compatibility_notes.append("LGBModel适合中等规模因子集(20-100)，当前因子数较多")
            elif factor_count < 10:
                compatibility_notes.append("LGBModel因子数较少，可能欠拟合")
            else:
                compatibility_notes.append("LGBModel与当前因子规模匹配良好")
        elif "NN" in model_type.upper() or "PT" in model_type.upper():
            if factor_count < 20:
                compatibility_notes.append("神经网络模型建议使用较多因子(>20)")
            else:
                compatibility_notes.append("神经网络模型与当前因子规模匹配")

        model_info["compatibility_notes"] = compatibility_notes
        model_info["found"] = True
        return model_info

    def _analyze_strategy(self, strategy_id: str,
                          custom_params: Optional[Dict] = None) -> Dict[str, Any]:
        """分析策略合理性。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT strategy_id, display_name, scenario, action,
                           portfolio_config, backtest_config
                    FROM aistock_strategy_catalog
                    WHERE strategy_id = %s
                """, (strategy_id,))
                row = cur.fetchone()

        if not row:
            return {"found": False, "strategy_id": strategy_id}

        cols = ["strategy_id", "display_name", "scenario", "action",
                "portfolio_config", "backtest_config"]
        strategy_info = dict(zip(cols, row))

        # 解析配置
        portfolio_config = strategy_info.get("portfolio_config") or {}
        if isinstance(portfolio_config, str):
            portfolio_config = json.loads(portfolio_config)

        warnings = []
        kwargs = portfolio_config.get("kwargs", {})

        # 检查TopkDropout参数合理性
        topk = kwargs.get("topk", 50)
        n_drop = kwargs.get("n_drop", 5)

        if custom_params:
            topk = custom_params.get("topk", topk)
            n_drop = custom_params.get("n_drop", n_drop)

        if topk > 100:
            warnings.append(f"topk={topk}偏高，持仓过于分散可能降低收益")
        if topk < 10:
            warnings.append(f"topk={topk}偏低，集中持仓风险较高")
        if n_drop > topk * 0.3:
            warnings.append(f"n_drop={n_drop}相对topk={topk}偏高，换手率可能过大")

        strategy_info["warnings"] = warnings
        strategy_info["found"] = True
        strategy_info["effective_params"] = {"topk": topk, "n_drop": n_drop}
        return strategy_info

    def _identify_risks(self, factor_analysis: Dict, model_analysis: Optional[Dict],
                        strategy_analysis: Optional[Dict]) -> List[Dict[str, str]]:
        """识别组合风险。"""
        risks = []

        # 因子风险
        categories = factor_analysis.get("categories", {})
        for cat, info in categories.items():
            if info["count"] > factor_analysis["count"] * 0.4:
                risks.append({
                    "level": "warning",
                    "type": "factor_concentration",
                    "message": f"因子类别'{cat}'占比过高({info['count']}/{factor_analysis['count']}), 可能存在冗余",
                })

        if factor_analysis.get("category_count", 0) < 3 and factor_analysis.get("count", 0) > 10:
            risks.append({
                "level": "warning",
                "type": "low_diversity",
                "message": "因子类别多样性不足，建议增加不同类别的因子",
            })

        if factor_analysis.get("avg_grade_score", 0) < 2.0:
            risks.append({
                "level": "info",
                "type": "low_quality",
                "message": "因子平均评级较低，建议替换低评级因子",
            })

        # 过拟合风险
        if factor_analysis.get("count", 0) > 80:
            risks.append({
                "level": "warning",
                "type": "overfitting",
                "message": f"因子数量({factor_analysis['count']})较多，存在过拟合风险",
            })

        return risks

    def _generate_suggestions(self, factor_analysis: Dict, model_analysis: Optional[Dict],
                              risks: List[Dict]) -> List[str]:
        """生成优化建议。"""
        suggestions = []

        if factor_analysis.get("diversity_score", 0) < 0.3:
            suggestions.append("建议增加不同类别的因子以提高多样性")

        if factor_analysis.get("avg_grade_score", 0) < 3.0:
            suggestions.append("建议用高评级因子替换低评级因子")

        if factor_analysis.get("count", 0) < 5:
            suggestions.append("因子数量较少，建议增加到10-30个以提高模型表现")

        if model_analysis and model_analysis.get("found"):
            if not model_analysis.get("is_sota"):
                suggestions.append("当前模型非SOTA，建议优先使用SOTA模型")

        if not suggestions:
            suggestions.append("当前组合配置合理，可以生成QLib配置进行回测")

        return suggestions

    def _compute_overall_score(self, factor_analysis: Dict, risks: List[Dict]) -> float:
        """计算综合评分(0-100)。"""
        score = 50.0  # 基础分

        # 因子质量加分
        avg_grade = factor_analysis.get("avg_grade_score", 0)
        score += avg_grade * 5  # 最多+25

        # 多样性加分
        diversity = factor_analysis.get("diversity_score", 0)
        score += diversity * 15  # 最多+15

        # 风险扣分
        for risk in risks:
            if risk["level"] == "warning":
                score -= 5
            elif risk["level"] == "critical":
                score -= 15

        return max(0, min(100, round(score, 1)))

    def _build_combination_for_profile(self, profile: str) -> Optional[Dict[str, Any]]:
        """根据目标特征构建推荐组合。"""
        from .factor_analyst import FactorAnalyst

        fa = FactorAnalyst()

        # 根据profile选择不同的因子策略
        if profile == "high_return":
            result = fa.recommend_factor_combination(
                target_count=30,
                min_grade="B",
                diversity_weight=0.3,
            )
            description = "高收益导向：选择高评级因子，侧重动量和ML因子"
        elif profile == "low_drawdown":
            result = fa.recommend_factor_combination(
                target_count=20,
                include_categories=["VOL", "STAT", "CORR", "LIQ"],
                min_grade="C",
                diversity_weight=0.7,
            )
            description = "低回撤导向：选择波动率和统计因子，注重多样性"
        elif profile == "high_sharpe":
            result = fa.recommend_factor_combination(
                target_count=25,
                min_grade="A",
                diversity_weight=0.5,
            )
            description = "高夏普导向：仅选择A级以上因子"
        else:  # balanced
            result = fa.recommend_factor_combination(
                target_count=25,
                min_grade="C",
                diversity_weight=0.6,
            )
            description = "均衡配置：兼顾收益和风险控制"

        if not result.get("factors"):
            return None

        factor_names = [f["factor_name"] for f in result["factors"]]

        # 选择最佳模型
        model_id = self._select_best_model()

        # 选择默认策略
        strategy_id = "topk_dropout_sota_factor"

        return {
            "profile": profile,
            "description": description,
            "factor_names": factor_names,
            "factor_count": len(factor_names),
            "category_summary": result.get("category_summary", {}),
            "model_id": model_id,
            "strategy_id": strategy_id,
        }

    def _select_best_model(self) -> Optional[str]:
        """选择最佳SOTA模型。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id FROM aistock_model_catalog
                    WHERE is_sota = TRUE
                    ORDER BY ic DESC NULLS LAST
                    LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    return row[0]

                # 如果没有SOTA，选IC最高的
                cur.execute("""
                    SELECT model_id FROM aistock_model_catalog
                    ORDER BY ic DESC NULLS LAST
                    LIMIT 1
                """)
                row = cur.fetchone()
                return row[0] if row else None

    # ============================================================
    # 智能生成：基于用户自然语言需求自动生成最佳组合
    # ============================================================

    def generate_from_requirement(
        self,
        user_requirement: str,
        use_llm: bool = True,
        max_factors: int = 30,
    ) -> Dict[str, Any]:
        """基于用户自然语言需求智能生成最佳因子+模型组合。

        流程：
        1. 收集因子库/模型库元数据摘要
        2. 解析用户需求 → 提取投资风格偏好
        3. 规则匹配 + LLM辅助选择最优因子/模型组合
        4. 评估组合并返回完整方案

        Args:
            user_requirement: 用户自然语言需求描述
            use_llm: 是否使用LLM辅助决策
            max_factors: 最大因子数量

        Returns:
            包含完整组合方案、评估结果和设计理由的字典
        """
        # ── 1. 收集因子库元数据 ──
        factor_metadata = self._get_factor_metadata_summary()
        model_metadata = self._get_model_metadata_summary()

        if not factor_metadata["factors"]:
            return {"ok": False, "error": "因子库为空，请先同步因子数据"}

        # ── 2. 解析用户需求 → 投资风格偏好 ──
        style_prefs = self._parse_requirement_to_style(user_requirement)

        # ── 3. 基于偏好选择因子 ──
        if use_llm:
            llm_result = self._generate_with_llm(
                user_requirement, factor_metadata, model_metadata, max_factors
            )
            if llm_result:
                # LLM返回了选择方案，验证并补充
                selected_factors = self._validate_llm_selection(
                    llm_result.get("factors", []), factor_metadata
                )
                selected_model = self._validate_model_selection(
                    llm_result.get("model_id"), model_metadata
                )
                design_rationale = llm_result.get("rationale", "")
                strategy_params = llm_result.get("strategy_params", {})
            else:
                # LLM失败，回退到规则
                selected_factors, selected_model, design_rationale, strategy_params = \
                    self._generate_by_rules(style_prefs, factor_metadata, model_metadata, max_factors)
        else:
            selected_factors, selected_model, design_rationale, strategy_params = \
                self._generate_by_rules(style_prefs, factor_metadata, model_metadata, max_factors)

        if not selected_factors:
            return {"ok": False, "error": "未能选出符合条件的因子"}

        # ── 4. 评估生成的组合 ──
        factor_names = [f["factor_name"] for f in selected_factors]
        evaluation = self.evaluate_combination(
            factor_names=factor_names,
            model_id=selected_model,
        )

        # ── 5. 组装结果 ──
        from .factor_analyst import FACTOR_CATEGORIES

        # 因子分类统计（含推荐理由）
        category_summary = {}
        for f in selected_factors:
            cat = f.get("category", "UNKNOWN")
            if cat not in category_summary:
                category_summary[cat] = {"count": 0, "factors": [], "category_name": ""}
            category_summary[cat]["count"] += 1
            category_summary[cat]["factors"].append(f["factor_name"])

        for cat in category_summary:
            category_summary[cat]["category_name"] = FACTOR_CATEGORIES.get(cat, cat)

        # 为每个因子添加推荐理由
        grade_label = {"S": "顶级", "A": "优秀", "B": "良好", "C": "合格", "D": "待改进"}
        factor_details_enriched = []
        for f in selected_factors:
            reason_parts = []
            g = f.get("grade", "")
            if g:
                reason_parts.append(f"评级{g}({grade_label.get(g, g)})")
            ic = f.get("ic_value")
            if ic is not None:
                reason_parts.append(f"IC={ic:.4f}")
            cat = f.get("category", "")
            if cat:
                cat_name = FACTOR_CATEGORIES.get(cat, cat)
                reason_parts.append(f"类别:{cat_name}")
            if cat in (style_prefs.get("preferred_categories") or []):
                reason_parts.append("匹配用户偏好类别")
            factor_details_enriched.append({
                **f,
                "selection_reason": "；".join(reason_parts) if reason_parts else "规则选择",
            })

        # 全量因子库类别概览
        all_categories_overview = {}
        for cat, cat_factors in factor_metadata["by_category"].items():
            cat_grade_dist = {}
            for cf in cat_factors:
                g = cf.get("grade", "D")
                cat_grade_dist[g] = cat_grade_dist.get(g, 0) + 1
            selected_in_cat = category_summary.get(cat, {}).get("count", 0)
            all_categories_overview[cat] = {
                "category_name": FACTOR_CATEGORIES.get(cat, cat),
                "total_factors": len(cat_factors),
                "selected_count": selected_in_cat,
                "grade_distribution": cat_grade_dist,
            }

        # 选中模型详情
        selected_model_info = None
        for m in model_metadata.get("models", []):
            if m.get("model_id") == selected_model:
                selected_model_info = m
                break

        # 分析思路说明
        analysis_steps = [
            f"1. 扫描因子库：共{factor_metadata['total']}个已评级因子，覆盖{len(factor_metadata['by_category'])}个类别",
            f"2. 解析投资需求：识别为{style_prefs.get('risk_appetite','moderate')}风格"
            + (f"，偏好类别：{'/'.join(FACTOR_CATEGORIES.get(c,c) for c in style_prefs.get('preferred_categories',[]))}" if style_prefs.get("preferred_categories") else "")
            + (f"，重点：{'/'.join(style_prefs.get('emphasis',[]))}" if style_prefs.get("emphasis") else ""),
            f"3. 因子筛选：最低评级{style_prefs.get('min_grade','C')}级，目标{style_prefs.get('target_factor_count',25)}个因子",
            "4. 多轮选择：偏好类别优先→类别多样性保证→评级排序填充→放宽限制补充",
            f"5. 最终选出{len(factor_names)}个因子，覆盖{len(category_summary)}个类别",
            f"6. 模型选择：{'SOTA模型' if selected_model_info and selected_model_info.get('is_sota') else '最优IC模型'}"
            + (f"（IC={selected_model_info.get('ic',0):.4f}）" if selected_model_info and selected_model_info.get("ic") else ""),
            f"7. 组合评估：综合评分{evaluation.get('overall_score', 0):.1f}分",
        ]

        return {
            "ok": True,
            "user_requirement": user_requirement,
            "style_preferences": style_prefs,
            "design_rationale": design_rationale,
            "analysis_steps": analysis_steps,
            "combination": {
                "factor_names": factor_names,
                "factor_count": len(factor_names),
                "factor_details": factor_details_enriched,
                "category_summary": category_summary,
                "model_id": selected_model,
                "model_info": selected_model_info,
                "strategy_params": strategy_params,
            },
            "evaluation": evaluation,
            "all_categories_overview": all_categories_overview,
            "metadata_summary": {
                "total_factors_available": factor_metadata["total"],
                "total_models_available": model_metadata["total"],
                "categories_available": list(factor_metadata["by_category"].keys()),
                "grade_distribution": factor_metadata.get("grade_distribution", {}),
            },
        }

    def _get_factor_metadata_summary(self) -> Dict[str, Any]:
        """获取因子库元数据摘要，供智能生成使用。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT factor_name, factor_source, category, grade,
                           ic_value, sharpe_value, ann_ret_value,
                           description, classification_reason
                    FROM qe_factor_classification
                    WHERE grade IS NOT NULL
                    ORDER BY
                        CASE grade
                            WHEN 'S' THEN 0 WHEN 'A' THEN 1 WHEN 'B' THEN 2
                            WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5
                        END ASC,
                        ic_value DESC NULLS LAST
                """)
                cols = [desc[0] for desc in cur.description]
                factors = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 按类别分组
        by_category = {}
        for f in factors:
            cat = f.get("category", "UNKNOWN")
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(f)

        # 评级分布
        grade_dist = {}
        for f in factors:
            g = f.get("grade", "D")
            grade_dist[g] = grade_dist.get(g, 0) + 1

        return {
            "total": len(factors),
            "factors": factors,
            "by_category": by_category,
            "grade_distribution": grade_dist,
        }

    def _get_model_metadata_summary(self) -> Dict[str, Any]:
        """获取模型库元数据摘要。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id, model_name, model_type, display_name,
                           ic, annualized_return, max_drawdown, information_ratio,
                           is_sota, description
                    FROM aistock_model_catalog
                    ORDER BY ic DESC NULLS LAST
                """)
                cols = [desc[0] for desc in cur.description]
                models = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {
            "total": len(models),
            "models": models,
            "sota_models": [m for m in models if m.get("is_sota")],
        }

    def _parse_requirement_to_style(self, requirement: str) -> Dict[str, Any]:
        """解析用户需求为投资风格偏好参数。"""
        req_lower = requirement.lower()

        prefs = {
            "risk_appetite": "moderate",  # conservative / moderate / aggressive
            "preferred_categories": [],
            "excluded_categories": [],
            "min_grade": "C",
            "target_factor_count": 25,
            "topk": 50,
            "n_drop": 5,
            "emphasis": [],  # 重点关注的维度
        }

        # 风险偏好
        _CONSERVATIVE_KW = ["保守", "稳健", "低风险", "低回撤", "防守", "安全",
                            "conservative", "low risk", "defensive", "stable"]
        _AGGRESSIVE_KW = ["激进", "高收益", "进攻", "高风险", "aggressive",
                          "high return", "offensive", "alpha"]

        if any(kw in req_lower for kw in _CONSERVATIVE_KW):
            prefs["risk_appetite"] = "conservative"
            prefs["min_grade"] = "B"
            prefs["target_factor_count"] = 20
            prefs["topk"] = 30
            prefs["emphasis"].append("低回撤")
        elif any(kw in req_lower for kw in _AGGRESSIVE_KW):
            prefs["risk_appetite"] = "aggressive"
            prefs["min_grade"] = "C"
            prefs["target_factor_count"] = 30
            prefs["topk"] = 50
            prefs["emphasis"].append("高收益")

        # 类别偏好
        _CATEGORY_KW = {
            "MOM": ["动量", "趋势", "momentum", "trend"],
            "VOL": ["波动率", "波动性", "低波动", "volatil"],
            "LIQ": ["流动性", "换手", "liquid", "turnover"],
            "VAL": ["价值", "估值", "基本面", "value", "fundamental"],
            "QUAL": ["质量", "盈利", "quality", "profit"],
            "MF": ["资金流", "主力", "资金", "money flow", "smart money"],
            "CHIP": ["筹码", "成本", "chip", "cost"],
            "TECH": ["技术", "指标", "technical"],
            "STAT": ["统计", "排名", "statistical"],
            "SIZE": ["规模", "市值", "size", "cap"],
        }
        for cat, keywords in _CATEGORY_KW.items():
            if any(kw in req_lower for kw in keywords):
                prefs["preferred_categories"].append(cat)

        # 因子数量偏好
        if "少量" in req_lower or "精选" in req_lower or "少" in req_lower:
            prefs["target_factor_count"] = min(prefs["target_factor_count"], 15)
        elif "大量" in req_lower or "全面" in req_lower or "多" in req_lower:
            prefs["target_factor_count"] = max(prefs["target_factor_count"], 35)

        # 夏普偏好
        if "夏普" in req_lower or "sharpe" in req_lower:
            prefs["min_grade"] = "A"
            prefs["emphasis"].append("高夏普")

        # 多样性偏好
        if "多样" in req_lower or "均衡" in req_lower or "分散" in req_lower or "balanced" in req_lower:
            prefs["emphasis"].append("多样性")

        return prefs

    def _generate_by_rules(
        self,
        style_prefs: Dict[str, Any],
        factor_metadata: Dict[str, Any],
        model_metadata: Dict[str, Any],
        max_factors: int,
    ) -> tuple:
        """基于规则的组合生成（不依赖LLM）。

        Returns:
            (selected_factors, model_id, design_rationale, strategy_params)
        """
        grade_order = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4}
        min_grade_val = grade_order.get(style_prefs.get("min_grade", "C"), 3)
        target_count = min(style_prefs.get("target_factor_count", 25), max_factors)
        preferred_cats = style_prefs.get("preferred_categories", [])
        risk_appetite = style_prefs.get("risk_appetite", "moderate")

        all_factors = factor_metadata["factors"]

        # 过滤：评级达标
        candidates = [
            f for f in all_factors
            if grade_order.get(f.get("grade", "D"), 4) <= min_grade_val
        ]

        if not candidates:
            candidates = all_factors[:max_factors]

        # 选择策略
        selected = []
        selected_names = set()
        max_per_category = max(2, int(target_count * 0.3))

        category_count = {}

        # 第一轮：优先选偏好类别的高评级因子
        if preferred_cats:
            for f in candidates:
                if len(selected) >= target_count:
                    break
                cat = f.get("category", "UNKNOWN")
                if cat in preferred_cats and f["factor_name"] not in selected_names:
                    if category_count.get(cat, 0) < max_per_category:
                        selected.append(f)
                        selected_names.add(f["factor_name"])
                        category_count[cat] = category_count.get(cat, 0) + 1

        # 第二轮：每个类别至少选1个（保证多样性）
        for cat, cat_factors in factor_metadata["by_category"].items():
            if len(selected) >= target_count:
                break
            if cat not in category_count:
                for f in cat_factors:
                    if f["factor_name"] not in selected_names:
                        if grade_order.get(f.get("grade", "D"), 4) <= min_grade_val:
                            selected.append(f)
                            selected_names.add(f["factor_name"])
                            category_count[cat] = 1
                            break

        # 第三轮：按评级填充剩余（受类别上限约束）
        for f in candidates:
            if len(selected) >= target_count:
                break
            if f["factor_name"] in selected_names:
                continue
            cat = f.get("category", "UNKNOWN")
            if category_count.get(cat, 0) >= max_per_category:
                continue
            selected.append(f)
            selected_names.add(f["factor_name"])
            category_count[cat] = category_count.get(cat, 0) + 1

        # 第四轮：若仍未填满，放宽类别上限继续填充
        if len(selected) < target_count:
            for f in candidates:
                if len(selected) >= target_count:
                    break
                if f["factor_name"] not in selected_names:
                    selected.append(f)
                    selected_names.add(f["factor_name"])
                    cat = f.get("category", "UNKNOWN")
                    category_count[cat] = category_count.get(cat, 0) + 1

        # 选择模型
        model_id = None
        if risk_appetite == "conservative":
            # 保守型：选IC最高的SOTA模型
            for m in model_metadata.get("sota_models", []):
                model_id = m["model_id"]
                break
        elif risk_appetite == "aggressive":
            # 激进型：选年化收益最高的模型
            sorted_models = sorted(
                model_metadata.get("models", []),
                key=lambda x: x.get("annualized_return") or 0,
                reverse=True,
            )
            if sorted_models:
                model_id = sorted_models[0]["model_id"]

        if not model_id:
            model_id = self._select_best_model()

        # 生成设计理由
        cat_names = []
        from .factor_analyst import FACTOR_CATEGORIES
        for cat in sorted(category_count.keys()):
            cat_names.append(f"{FACTOR_CATEGORIES.get(cat, cat)}({category_count[cat]}个)")

        rationale_parts = [f"基于{risk_appetite}风格偏好"]
        if preferred_cats:
            pref_names = [FACTOR_CATEGORIES.get(c, c) for c in preferred_cats]
            rationale_parts.append(f"重点配置{'/'.join(pref_names)}")
        rationale_parts.append(f"共选择{len(selected)}个因子，覆盖{len(category_count)}个类别：{', '.join(cat_names)}")
        rationale_parts.append(f"最低评级要求{style_prefs.get('min_grade', 'C')}级")
        design_rationale = "。".join(rationale_parts)

        strategy_params = {
            "topk": style_prefs.get("topk", 50),
            "n_drop": style_prefs.get("n_drop", 5),
        }

        return selected, model_id, design_rationale, strategy_params

    def _generate_with_llm(
        self,
        user_requirement: str,
        factor_metadata: Dict[str, Any],
        model_metadata: Dict[str, Any],
        max_factors: int,
    ) -> Optional[Dict[str, Any]]:
        """使用LLM辅助生成最优组合。"""
        try:
            import litellm
        except ImportError:
            logger.warning("litellm未安装，回退到规则生成")
            return None

        # 构建因子库摘要（按类别汇总，避免token过长）
        from .factor_analyst import FACTOR_CATEGORIES
        factor_summary_lines = []
        for cat, cat_factors in factor_metadata["by_category"].items():
            cat_name = FACTOR_CATEGORIES.get(cat, cat)
            grade_counts = {}
            for f in cat_factors:
                g = f.get("grade", "D")
                grade_counts[g] = grade_counts.get(g, 0) + 1
            grade_str = ", ".join(f"{g}级{c}个" for g, c in sorted(grade_counts.items()))
            top_factors = [f["factor_name"] for f in cat_factors[:5]]
            factor_summary_lines.append(
                f"  {cat}({cat_name}): 共{len(cat_factors)}个因子, {grade_str}. "
                f"代表因子: {', '.join(top_factors)}"
            )
        factor_summary = "\n".join(factor_summary_lines)

        # 构建模型库摘要
        model_summary_lines = []
        for m in model_metadata["models"][:10]:
            sota_tag = " [SOTA]" if m.get("is_sota") else ""
            ic_str = f"IC={m['ic']:.4f}" if m.get("ic") else "IC=N/A"
            ar_str = f"年化={m['annualized_return']:.2%}" if m.get("annualized_return") else "年化=N/A"
            model_summary_lines.append(
                f"  {m['model_id']}: {m.get('model_name', 'N/A')}{sota_tag}, {ic_str}, {ar_str}"
            )
        model_summary = "\n".join(model_summary_lines)

        # 构建所有可用因子名称列表（按类别分组）
        available_factors_lines = []
        for cat, cat_factors in factor_metadata["by_category"].items():
            cat_name = FACTOR_CATEGORIES.get(cat, cat)
            names = [f"{f['factor_name']}({f.get('grade','?')})" for f in cat_factors]
            available_factors_lines.append(f"  {cat}({cat_name}): {', '.join(names)}")
        available_factors = "\n".join(available_factors_lines)

        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("portfolio_architect", "smart_select")
        
        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = safe_format(
                prompt_data["user_prompt_template"],
                user_requirement=user_requirement,
                max_factors=max_factors,
                total_factors=len(factor_metadata["factors"]),
                factor_summary=factor_summary,
                available_factors=available_factors,
                model_summary=model_summary
            )
        else:
            raise ValueError("未配置 portfolio_architect/smart_select 的提示词，拒绝使用兜底策略")

        try:
            from .llm_client import get_llm_kwargs
            kwargs = get_llm_kwargs("portfolio_architect")
            
            response = litellm.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=2000,
                **kwargs
            )
            content = response.choices[0].message.content.strip()
            # 解析JSON
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            result = json.loads(content)
            logger.info(f"LLM生成组合: {len(result.get('factors', []))}个因子, 模型={result.get('model_id')}")
            return result
        except Exception as e:
            logger.exception(f"LLM组合生成失败: {e}")
            return None

    def _validate_llm_selection(
        self,
        llm_factors: List[str],
        factor_metadata: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """验证LLM选择的因子是否存在于因子库中。"""
        available = {f["factor_name"]: f for f in factor_metadata["factors"]}
        validated = []
        for name in llm_factors:
            if name in available:
                validated.append(available[name])
            else:
                logger.warning(f"LLM选择的因子 '{name}' 不在因子库中，已跳过")
        return validated

    def _validate_model_selection(
        self,
        llm_model_id: Optional[str],
        model_metadata: Dict[str, Any],
    ) -> Optional[str]:
        """验证LLM选择的模型是否存在。"""
        if not llm_model_id:
            return self._select_best_model()
        available_ids = {m["model_id"] for m in model_metadata["models"]}
        if llm_model_id in available_ids:
            return llm_model_id
        logger.warning(f"LLM选择的模型 '{llm_model_id}' 不存在，回退到最佳模型")
        return self._select_best_model()
