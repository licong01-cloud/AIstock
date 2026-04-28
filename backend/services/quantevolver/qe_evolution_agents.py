import logging
import json
import asyncio
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List

from .factor_official_evaluation_service import CALC_ENGINE
from .llm_client import get_llm_kwargs

try:
    import litellm
except ImportError:
    litellm = None

logger = logging.getLogger(__name__)

_ACTIVE_OFFICIAL_RATING_JOIN_SQL = """
JOIN aistock_factor_catalog fc
  ON c.factor_name = fc.factor_name AND c.factor_source = fc.source
LEFT JOIN LATERAL (
    SELECT official_grade, official_score, rule_version
    FROM qe_factor_official_ratings r
    WHERE r.factor_catalog_id = fc.id
      AND r.rule_version = (
          SELECT rule_version FROM qe_rating_rule_versions
          WHERE status = 'active'
          ORDER BY activated_at DESC NULLS LAST, created_at DESC
          LIMIT 1
      )
    ORDER BY r.graded_at DESC
    LIMIT 1
) fr ON TRUE
"""

_IND_RANK_IC_BEST_ABS_SQL = """
CASE WHEN m.rank_ic_1d IS NULL AND m.rank_ic_5d IS NULL
          AND m.rank_ic_10d IS NULL AND m.rank_ic_20d IS NULL THEN NULL
     ELSE GREATEST(COALESCE(ABS(m.rank_ic_1d), 0),
                   COALESCE(ABS(m.rank_ic_5d), 0),
                   COALESCE(ABS(m.rank_ic_10d), 0),
                   COALESCE(ABS(m.rank_ic_20d), 0))
END
"""


@dataclass
class AnalystResult:
    """Analyst 两步 LLM 的输出结构。"""
    report_text: str = ""               # Step 1 诊断报告（人类可读）
    diagnosis: Dict[str, Any] = field(default_factory=dict)  # Step 1 结构化诊断信号
    direction: Dict[str, Any] = field(default_factory=dict)  # Step 2 方向决策

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class EvolutionAgents:
    """
    包含自动演进流程中使用的各个 Agent 角色调用逻辑。
    负责通过 llm_client 调取大模型进行分析和决策。
    """

    def __init__(self):
        self._llm_trace: List[Dict[str, Any]] = []

    def reset_trace(self):
        """每轮 Loop 开始前清空 trace。"""
        self._llm_trace = []

    def get_trace(self) -> List[Dict[str, Any]]:
        """获取当前 Loop 的完整 LLM 调用记录。"""
        return self._llm_trace

    def _call_llm(self, agent_type: str, system_prompt: str, user_prompt: str, step: str = "") -> str:
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
                timeout=1800,  # 30 minutes for long SOTA evaluation
                **kwargs
            )
            content = response.choices[0].message.content
            # 记录 LLM 调用 trace
            from datetime import datetime
            self._llm_trace.append({
                "step": step or agent_type,
                "agent_type": agent_type,
                "prompt_summary": user_prompt[:300],
                "raw_response": content,
                "timestamp": datetime.now().isoformat(),
            })
            return content
        except Exception as e:
            logger.error(f"LLM call failed for {agent_type}: {e}")
            raise

    async def async_call_llm(self, agent_type: str, system_prompt: str, user_prompt: str, step: str = "") -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._call_llm, agent_type, system_prompt, user_prompt, step)
        
    async def run_analyst(
        self,
        loop_index: int,
        config: Dict[str, Any],
        metrics: Dict[str, Any],
        analysis_context: Optional[Dict[str, Any]] = None,
        evolution_history: Optional[Dict[str, Any]] = None,
    ) -> AnalystResult:
        """
        两步 Analyst Agent:
        Step 1: 诊断分析 → report_text + structured_diagnosis
        Step 2: 方向决策 → recommended_direction JSON
        """
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        ctx = analysis_context or {}
        hist = evolution_history or {}
        trend_summary = hist.get("trend_summary", {})

        # ── Step 1: 诊断分析 ──
        prompt_data = pm.get_active_prompt_text("evolution_analyst", "diagnose_experiment")
        if not prompt_data:
            raise ValueError("未配置 evolution_analyst/diagnose_experiment 的提示词")

        # 提取训练诊断 → analysis_context 传入 (S6)
        enhanced = metrics.get("enhanced_metrics", {})
        training_diagnostics = enhanced.get("training_diagnostics", {})

        # 剥离 enhanced_metrics（含 stock_trades/return_curves 等大量数据），避免超出 context window
        metrics_for_prompt = {k: v for k, v in metrics.items() if k != "enhanced_metrics"}
        system_prompt = prompt_data["system_prompt"]
        user_prompt = safe_format(prompt_data["user_prompt_template"],
            loop_index=loop_index,
            config=json.dumps(config, ensure_ascii=False),
            metrics=json.dumps(metrics_for_prompt, ensure_ascii=False),
            training_diagnostics=json.dumps(training_diagnostics, ensure_ascii=False) if training_diagnostics else "无训练诊断数据",
            factor_profiles=json.dumps(ctx.get("factor_profiles", {}), ensure_ascii=False),
            target_desc=ctx.get("target_desc", ""),
            evolution_guidance=ctx.get("evolution_guidance", ""),
            ic_trend=json.dumps(trend_summary.get("ic_trend", [])),
            best_ic=trend_summary.get("best_ic", "N/A"),
            total_loops=hist.get("total_loops", 0),
            failed_approaches=json.dumps(hist.get("failed_approaches", [])[-5:], ensure_ascii=False),
        )

        step1_resp = await self.async_call_llm("evolution_analyst", system_prompt, user_prompt, step="analyst_diagnose")

        # 解析 Step 1 输出：报告 + ---STRUCTURED_DIAGNOSIS--- + JSON
        report_text = step1_resp
        diagnosis = {}
        separator = "---STRUCTURED_DIAGNOSIS---"
        if separator in step1_resp:
            parts = step1_resp.split(separator, 1)
            report_text = parts[0].strip()
            json_part = parts[1].strip()
            try:
                start = json_part.find("{")
                end = json_part.rfind("}")
                if start != -1 and end != -1:
                    diagnosis = json.loads(json_part[start:end + 1])
            except Exception as e:
                raise RuntimeError(f"Analyst Step 1 diagnosis JSON parse failed: {e}") from e

        # ── Step 2: 方向决策 ──
        step2_data = pm.get_active_prompt_text("evolution_analyst", "decide_direction")
        if not step2_data:
            raise ValueError("未配置 evolution_analyst/decide_direction 的提示词，拒绝使用兜底策略")

        consecutive = trend_summary.get("consecutive_same_action", {})
        try:
            step2_system = step2_data["system_prompt"]
            step2_user = safe_format(step2_data["user_prompt_template"],
                structured_diagnosis=json.dumps(diagnosis, ensure_ascii=False),
                consecutive_action_type=consecutive.get("action_type", "N/A"),
                consecutive_count=consecutive.get("count", 0),
                consecutive_ic_deltas=json.dumps(consecutive.get("recent_ic_deltas", [])),
                unexplored_directions=json.dumps(trend_summary.get("unexplored_directions", [])),
                action_type_stats=json.dumps(trend_summary.get("action_type_stats", {}), ensure_ascii=False),
                evolution_mode=ctx.get("evolution_mode", "auto"),
            )
            step2_resp = await self.async_call_llm("evolution_analyst", step2_system, step2_user, step="analyst_direction")
            start = step2_resp.find("{")
            end = step2_resp.rfind("}")
            if start != -1 and end != -1:
                direction = json.loads(step2_resp[start:end + 1])
            else:
                raise ValueError("Analyst Step 2 LLM 返回中未找到有效 JSON")
        except (ValueError, RuntimeError):
            raise
        except Exception as e:
            raise RuntimeError(f"Analyst Step 2 方向决策 LLM 调用失败: {e}") from e

        return AnalystResult(
            report_text=report_text,
            diagnosis=diagnosis,
            direction=direction,
        )

    async def run_evaluator(
        self,
        current_metrics: Dict[str, Any],
        historical_sota_metrics: Optional[Dict[str, Any]],
        evolution_history: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        SOTA 评估官 — 硬性否决 → LLM 判断 → 安全网兜底
        返回 dict: {"is_sota": bool, "reason": str, "method": str}
        method: "baseline" | "hard_veto" | "llm" | "safety_net"
        """
        if not historical_sota_metrics:
            _BASELINE_IC = 0.03
            _BASELINE_ANN_RET = 0.10
            _BASELINE_MAX_DD = 0.30
            cur_ic = current_metrics.get("IC") or 0
            cur_ret = current_metrics.get("annualized_return") or 0
            cur_dd_raw = current_metrics.get("max_drawdown")
            cur_dd = abs(cur_dd_raw) if cur_dd_raw is not None else None
            meets_baseline = (
                cur_ic > _BASELINE_IC
                and cur_ret > _BASELINE_ANN_RET
                and cur_dd is not None
                and cur_dd < _BASELINE_MAX_DD
            )
            dd_str = f"{cur_dd:.1%}" if cur_dd is not None else "N/A"
            if meets_baseline:
                reason = f"首个 SOTA 达标: IC={cur_ic:.4f}, AnnRet={cur_ret:.1%}, MaxDD={dd_str}"
                logger.info(reason)
                return {"is_sota": True, "reason": reason, "method": "baseline"}
            else:
                reason = f"未达 SOTA 基线 (IC={cur_ic:.4f}, AnnRet={cur_ret:.1%}, MaxDD={dd_str})"
                logger.info(reason)
                return {"is_sota": False, "reason": reason, "method": "baseline"}

        # ── 计算核心指标变化率 ──
        cur_ret = current_metrics.get("annualized_return", 0) or 0
        sota_ret = historical_sota_metrics.get("annualized_return", 0) or 0
        cur_dd_raw = current_metrics.get("max_drawdown")
        sota_dd_raw = historical_sota_metrics.get("max_drawdown")
        dd_available = cur_dd_raw is not None and sota_dd_raw is not None and cur_dd_raw != 0 and sota_dd_raw != 0
        cur_dd = abs(cur_dd_raw) if dd_available else None
        sota_dd = abs(sota_dd_raw) if dd_available else None
        ret_change = (cur_ret - sota_ret) / abs(sota_ret) if sota_ret != 0 else 0
        dd_change = (cur_dd - sota_dd) / sota_dd if dd_available else 0
        cur_sharpe = current_metrics.get("sharpe", 0) or 0
        sota_sharpe = historical_sota_metrics.get("sharpe", 0) or 0
        sharpe_change = (cur_sharpe - sota_sharpe) / abs(sota_sharpe) if sota_sharpe != 0 else 0

        # ── 第一层: 硬性否决（LLM 无权推翻） ──
        if ret_change < -0.20:
            reason = f"硬性否决: AnnRet 退化 {ret_change*100:.1f}%"
            logger.info(f"SOTA {reason}")
            return {"is_sota": False, "reason": reason, "method": "hard_veto"}
        if dd_available and dd_change > 0.30:
            reason = f"硬性否决: MaxDD 恶化 {dd_change*100:.1f}%"
            logger.info(f"SOTA {reason}")
            return {"is_sota": False, "reason": reason, "method": "hard_veto"}
        degradation_count = 0
        for key, cur_v, sota_v in [
            ("IC", current_metrics.get("IC", 0) or 0, historical_sota_metrics.get("IC", 0) or 0),
            ("ICIR", current_metrics.get("ICIR", 0) or 0, historical_sota_metrics.get("ICIR", 0) or 0),
            ("AnnRet", cur_ret, sota_ret),
            ("Sharpe", cur_sharpe, sota_sharpe),
        ]:
            if sota_v > 0 and cur_v < sota_v * 0.90:
                degradation_count += 1
        if degradation_count >= 3:
            reason = f"硬性否决: {degradation_count}/4 指标退化 >10%"
            logger.info(f"SOTA {reason}")
            return {"is_sota": False, "reason": reason, "method": "hard_veto"}

        # ── 第二层: LLM 判断（主判断） ──
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_evaluator", "evaluate_sota")
        if not prompt_data:
            raise ValueError("未配置 evolution_evaluator/evaluate_sota 的提示词")

        try:
            system_prompt = prompt_data["system_prompt"]
            trimmed_eval_history: Dict[str, Any] = {}
            if evolution_history:
                trimmed_eval_history["total_loops"] = evolution_history.get("total_loops", 0)
                trimmed_eval_history["sota_loop_index"] = evolution_history.get("sota_loop_index")
                trimmed_eval_history["trend_summary"] = evolution_history.get("trend_summary", {})
                loops = evolution_history.get("loops", [])
                trimmed_eval_history["loops"] = [
                    {k: v for k, v in lp.items() if k in ("loop_index", "action_type", "IC", "annualized_return", "sharpe", "max_drawdown", "is_sota")}
                    for lp in loops[-3:]
                ]

            # 清理 metrics 中的大体积字段，避免干扰 LLM 判断
            def _strip_heavy_fields(m: Dict) -> Dict:
                return {k: v for k, v in m.items() if k != "enhanced_metrics"}

            user_prompt = safe_format(prompt_data["user_prompt_template"],
                current_metrics=json.dumps(_strip_heavy_fields(current_metrics), ensure_ascii=False),
                historical_sota_metrics=json.dumps(_strip_heavy_fields(historical_sota_metrics), ensure_ascii=False),
                evolution_history=json.dumps(trimmed_eval_history, ensure_ascii=False),
            )

            max_retries = 2
            llm_result = None
            llm_reason = ""
            llm_path = ""
            for attempt in range(max_retries + 1):
                try:
                    response = await self.async_call_llm("evolution_evaluator", system_prompt, user_prompt, step="evaluator_sota")
                    start = response.find("{")
                    end = response.rfind("}")
                    if start != -1 and end != -1:
                        payload = json.loads(response[start:end+1])
                        if "is_sota" in payload:
                            llm_result = bool(payload.get("is_sota"))
                            llm_reason = str(payload.get("reason", ""))[:300]
                            llm_path = str(payload.get("path", ""))
                            logger.info(f"LLM SOTA: {llm_result} (path={llm_path}, reason={llm_reason[:100]})")
                            break
                    raise ValueError(f"LLM 返回无法解析为 SOTA 判断: {response[:200]}")
                except Exception as e:
                    error_type = type(e).__name__
                    is_timeout = "timeout" in error_type.lower() or "timeout" in str(e).lower()

                    if is_timeout and attempt < max_retries:
                        wait_time = 10 * (attempt + 1)
                        logger.warning(f"SOTA 评估超时 (attempt {attempt+1}/{max_retries+1}), 等待 {wait_time}s 后重试: {e}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise RuntimeError(f"SOTA 评估 LLM 调用失败，任务中止: {e}") from e

            if llm_result is True:
                return {"is_sota": True, "reason": llm_reason, "path": llm_path, "method": "llm"}

            # ── 第三层: 安全网 — 覆盖 LLM 的明显误判 ──
            # 当核心绩效指标（AnnRet + Sharpe）均显著改善且 MaxDD 未严重恶化时，
            # LLM 拒绝可能是受到噪声指标干扰，安全网介入纠正
            dd_ok = not dd_available or dd_change < 0.15  # MaxDD 恶化 <15%
            if ret_change >= 0.03 and sharpe_change >= 0.03 and dd_ok:
                reason = (
                    f"安全网覆盖 LLM 误判: AnnRet↑{ret_change*100:.1f}%, "
                    f"Sharpe↑{sharpe_change*100:.1f}%, "
                    f"MaxDD变化{dd_change*100:+.1f}% (LLM reason: {llm_reason[:100]})"
                )
                logger.warning(f"SOTA {reason}")
                return {"is_sota": True, "reason": reason, "path": "safety_net", "method": "safety_net"}

            return {"is_sota": False, "reason": llm_reason, "path": llm_path, "method": "llm"}

        except Exception as e:
            raise RuntimeError(f"SOTA 评估异常: {e}") from e

    async def run_researcher(
        self,
        analyst_report: str,
        sota_status: bool,
        current_config: Dict[str, Any],
        evolution_history: Optional[Dict[str, Any]] = None,
        researcher_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        演进策略研究员
        """
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_researcher", "propose_config")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            # 精简 evolution_history：去掉 enhanced_metrics（含 top_stocks/stock_trades 等大体积数据）
            # 避免随 loop 增多导致 context window 超限
            _CORE_METRIC_KEYS = ("IC", "Rank_IC", "ICIR", "Rank_ICIR", "cagr", "sharpe",
                                 "final_nav", "max_drawdown", "annualized_return", "n_trading_days")
            def _trim_loop_metrics(lp: Dict[str, Any]) -> Dict[str, Any]:
                m = lp.get("metrics", {})
                return {
                    **{k: v for k, v in lp.items() if k != "metrics"},
                    "metrics": {k: m[k] for k in _CORE_METRIC_KEYS if k in m},
                }
            hist = evolution_history or {}
            trimmed_history = {
                **{k: v for k, v in hist.items() if k != "loops"},
                "loops": [_trim_loop_metrics(lp) for lp in hist.get("loops", [])],
            }
            user_prompt = safe_format(prompt_data["user_prompt_template"],
                analyst_report=analyst_report,
                sota_status=sota_status,
                current_config=json.dumps(current_config, ensure_ascii=False),
                evolution_history=json.dumps(trimmed_history, ensure_ascii=False),
                researcher_context=json.dumps(researcher_context or {}, ensure_ascii=False),
            )
        else:
            raise ValueError("未配置 evolution_researcher/propose_config 的提示词，拒绝使用兜底策略")
            
        response = await self.async_call_llm("evolution_researcher", system_prompt, user_prompt, step="researcher_propose")
        
        start = response.find("{")
        end = response.rfind("}")
        if start != -1 and end != -1:
            try:
                return json.loads(response[start:end+1])
            except Exception as e:
                raise ValueError(f"Researcher JSON parse failed: {e}. Raw: {response}") from e
        raise ValueError(f"Researcher output contains no JSON object: {response}")

    async def run_reviewer(
        self,
        draft_config: Dict[str, Any],
        evolution_history: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        配置审查与构建员
        目前简化为结构校验，确保能发给 RDAgent。
        """
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("evolution_reviewer", "review_config")
        if not prompt_data:
            raise ValueError("未配置 evolution_reviewer/review_config 的提示词，拒绝使用兜底策略")

        MAX_RETRIES = 2  # 总共尝试 3 次 (1 + 2 retry)
        last_rejection_reason = None

        # 截断 evolution_history，Reviewer 只需最近 3 轮摘要
        trimmed_history = {}
        if evolution_history:
            trimmed_history["total_loops"] = evolution_history.get("total_loops", 0)
            trimmed_history["sota_loop_index"] = evolution_history.get("sota_loop_index")
            loops = evolution_history.get("loops", [])
            recent = loops[-3:] if len(loops) > 3 else loops
            trimmed_history["loops"] = [
                {
                    "loop_index": lp.get("loop_index"),
                    "action_type": lp.get("action_type"),
                    "is_sota": lp.get("is_sota"),
                    "metrics": {k: lp.get("metrics", {}).get(k)
                                for k in ("IC", "Rank_IC", "ICIR", "sharpe", "annualized_return", "max_drawdown")
                                if lp.get("metrics", {}).get(k) is not None},
                }
                for lp in recent
            ]

        for attempt in range(MAX_RETRIES + 1):
            try:
                system_prompt = prompt_data["system_prompt"]
                user_prompt = safe_format(prompt_data["user_prompt_template"],
                    draft_config=json.dumps(draft_config, ensure_ascii=False),
                    evolution_history=json.dumps(trimmed_history, ensure_ascii=False),
                )
                # 重试时注入上次拒绝原因
                if last_rejection_reason and attempt > 0:
                    user_prompt += (
                        f"\n\n【重试请求】上次审查被拒绝，原因: {last_rejection_reason}\n"
                        f"请重新审查并尽量批准，或给出具体的修改建议。"
                        f"这是第 {attempt + 1} 次审查。"
                    )
                response = await self.async_call_llm("evolution_reviewer", system_prompt, user_prompt, step="reviewer_validate")

                start = response.find("{")
                end = response.rfind("}")
                if start != -1 and end != -1:
                    payload = json.loads(response[start:end+1])
                    approved = bool(payload.get("approved", False))
                    if not approved:
                        last_rejection_reason = payload.get("reason", "Reviewer rejected config")
                        if attempt < MAX_RETRIES:
                            logger.warning(f"Reviewer 拒绝 (attempt {attempt+1}/{MAX_RETRIES+1}): {last_rejection_reason}, 重试...")
                            continue
                        else:
                            logger.warning(f"Reviewer 连续 {MAX_RETRIES+1} 次拒绝，跳过 LLM 审批，仅做结构校验")
                            break  # 跳出循环，走结构校验
                    candidate = payload.get("config")
                    if isinstance(candidate, dict):
                        draft_config = candidate
                break  # 成功或无 JSON 时跳出，走结构校验
            except Exception as e:
                raise RuntimeError(f"evolution_reviewer 提示词调用或解析失败 (attempt {attempt+1}): {e}") from e

        required_keys = ["model_params", "factor_list", "action_type"]
        missing = [k for k in required_keys if k not in draft_config]
        if missing:
            raise ValueError(f"Reviewer rejected config, missing required keys: {missing}")

        if not isinstance(draft_config["model_params"], dict):
            raise ValueError("Reviewer rejected config: model_params must be an object")
        if not isinstance(draft_config["factor_list"], list):
            raise ValueError("Reviewer rejected config: factor_list must be an array")

        action_type = draft_config["action_type"]
        allowed_action_types = {"initial", "factor_adjust", "param_tune", "model_switch", "factor_model_joint", "factor_expand"}
        if not isinstance(action_type, str) or action_type not in allowed_action_types:
            raise ValueError(
                f"Reviewer rejected config: action_type must be one of {sorted(allowed_action_types)}"
            )

        return draft_config


class EvolutionFactorAgent:
    """
    7步因子编排器 — 替代 run_researcher 做因子组合演进决策。

    Step 1: analyze_experiment_result [工具] — 分析上轮结果
    Step 2: decide_direction [LLM#1] — 探索方向 (<4000 tokens)
    Step 3: search_candidate_factors [工具] — 加权随机抽样
    Step 4: screen_candidates [LLM#2] — 筛选15个 (<4000 tokens)
    Step 5: fetch_factor_details [工具] — 获取详细数据+相关性
    Step 6: design_combination [LLM#3] — 设计最终组合 (<8000 tokens)
    Step 7: validate_config [工具] — 规则校验
    """

    def __init__(self, agents: Optional["EvolutionAgents"] = None):
        self._agents = agents or EvolutionAgents()

    def _get_factor_ic_map(self, factor_names: List[str]) -> Dict[str, float]:
        """查询因子最新独立 IC，返回 {factor_name: ic_mean}，用于淘汰优先级排序。"""
        if not factor_names:
            return {}
        from ...db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT DISTINCT ON (factor_name) factor_name, ic_mean
                    FROM aistock_factor_metrics
                    WHERE factor_name IN ({ph}) AND eval_window = 'full' AND calc_engine = %s
                    ORDER BY factor_name, calculated_at DESC
                """, [*factor_names, CALC_ENGINE])
                return {row[0]: float(row[1]) if row[1] is not None else 0.0
                        for row in cur.fetchall()}

    def _get_factor_importance_map(
        self, factor_names: List[str], evolution_history: Optional[Dict[str, Any]] = None,
    ) -> tuple:
        """
        获取因子重要性。优先用上轮模型内 feature_importance，回退到独立 IC。
        返回 (importance_map, method)。
        """
        # 尝试从上轮 enhanced_metrics 获取
        if evolution_history and evolution_history.get("loops"):
            last_loop = evolution_history["loops"][-1]
            em = last_loop.get("metrics", {}).get("enhanced_metrics", {})
            fi = em.get("feature_importance", {})
            factors_data = fi.get("factors", {})
            if factors_data:
                result = {}
                for fn in factor_names:
                    result[fn] = factors_data.get(fn, {}).get("importance", 0.0)
                return result, fi.get("method", "model_importance")

        # 回退: 独立 IC
        ic_map = self._get_factor_ic_map(factor_names)
        if not ic_map:
            logger.warning("因子重要性回退到 uniform（无模型内 importance 且无独立 IC 数据）")
            return {fn: 1.0 / max(len(factor_names), 1) for fn in factor_names}, "uniform"
        logger.warning("因子重要性回退到 independent_ic（无模型内 feature_importance，使用独立 IC 代替）")
        total = sum(abs(v) for v in ic_map.values()) or 1.0
        return {fn: abs(ic_map.get(fn, 0.0)) / total for fn in factor_names}, "independent_ic"

    def _get_factor_categories(self, factor_names: List[str]) -> set:
        """查询因子列表对应的类别集合，用于 factor_expand 时识别未覆盖类别。"""
        if not factor_names:
            return set()
        from ...db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT DISTINCT category FROM qe_factor_classification
                    WHERE factor_name IN ({ph}) AND category IS NOT NULL
                """, factor_names)
                return {row[0] for row in cur.fetchall()}

    async def run(
        self,
        analyst_result: 'AnalystResult',
        is_sota: bool,
        current_config: Dict[str, Any],
        evolution_history: Optional[Dict[str, Any]] = None,
        researcher_context: Optional[Dict[str, Any]] = None,
        sota_protected_factors: Optional[List[str]] = None,
        sota_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行 7 步编排，返回最终 config（与 run_researcher 返回格式兼容）。

        Args:
            analyst_result: Analyst 两步 LLM 输出（AnalystResult 对象）。
            sota_protected_factors: [已弃用] 因子保护已由 importance-based 机制替代，
                                   此参数保留仅为 API 兼容，内部不使用。
            sota_context: SOTA 回滚上下文，包含本轮和 SOTA 两组数据。
        """
        from ...db.pg_pool import get_conn
        analyst_report = analyst_result.report_text

        # ── Step 1: analyze_experiment_result ──
        current_factors = current_config.get("factor_list", [])
        current_metrics = {}
        if evolution_history and evolution_history.get("loops"):
            last_loop = evolution_history["loops"][-1]
            current_metrics = last_loop.get("metrics", {})

        step1_summary = {
            "current_factor_count": len(current_factors),
            "current_ic": current_metrics.get("IC"),
            "current_sharpe": current_metrics.get("sharpe"),
            "is_sota": is_sota,
            "trend": evolution_history.get("trend_summary", {}).get("ic_trend", []) if evolution_history else [],
            "failed_approaches": evolution_history.get("failed_approaches", []) if evolution_history else [],
        }
        logger.info(f"EvolutionFactorAgent Step 1: {step1_summary}")

        # ── Step 2: decide_direction [LLM#1] — 从 DB 加载提示词 (F1: 不输出 action_type) ──
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()

        # 因子重要性驱动的保护机制（替代旧版 SOTA 因子保护）
        importance_map, imp_method = self._get_factor_importance_map(current_factors, evolution_history)
        sorted_factors = sorted(importance_map.items(), key=lambda x: -x[1])
        n_total = len(sorted_factors)
        if n_total < 15:
            # 小因子集：保护降到50%，放宽turnover到最少3个，鼓励探索
            n_protected = max(1, int(n_total * 0.5))
            max_turnover = max(3, int(n_total * 0.5))
        else:
            n_protected = max(1, int(n_total * 0.7))
            max_turnover = max(1, int(n_total * 0.3))
        protected_set = {f for f, _ in sorted_factors[:n_protected]}

        importance_lines = []
        for fn, imp in sorted_factors:
            tag = "[受保护]" if fn in protected_set else "[可淘汰]"
            importance_lines.append(f"  - {fn}: importance={imp:.4f} {tag}")

        importance_note = (
            f"\n\n【因子模型内重要性】(方法: {imp_method})\n"
            + "\n".join(importance_lines)
            + f"\n\nTop {n_protected} 个因子受保护，Bottom {n_total - n_protected} 个可替换。"
            + f"本轮最多替换 {max_turnover} 个因子。"
        )
        # SOTA 回滚上下文注入
        sota_note = ""
        if sota_context and sota_context.get("rollback_applied"):
            cr = sota_context["current_run"]
            sb = sota_context["sota_baseline"]
            sota_note = (
                f"\n\n【SOTA 回滚上下文】本轮非 SOTA，已回滚到 SOTA 配置。"
                f"\n本轮实际配置: {len(cr['config'].get('factor_list', []))} 因子, "
                f"IC={cr['metrics'].get('IC', 'N/A')}, AnnRet={cr['metrics'].get('annualized_return', 'N/A')}"
                f"\nSOTA 基线: {len(sb['config'].get('factor_list', []))} 因子, "
                f"IC={sb['metrics'].get('IC', 'N/A')}, AnnRet={sb['metrics'].get('annualized_return', 'N/A')}"
                f"\n请从 SOTA 基线出发改进，参考本轮失败经验避免重复。"
            )
        effective_report = analyst_report[:2500] + importance_note + sota_note

        step2_prompt_data = pm.get_active_prompt_text("evolution_factor_agent", "decide_direction")
        if not step2_prompt_data:
            raise ValueError("未配置 evolution_factor_agent/decide_direction 的提示词，拒绝使用兜底策略")
        direction_system = step2_prompt_data["system_prompt"]
        direction_prompt = safe_format(step2_prompt_data["user_prompt_template"],
            analyst_report=effective_report,
            factor_count=len(current_factors),
            current_ic=current_metrics.get('IC', 'N/A'),
            current_sharpe=current_metrics.get('sharpe', 'N/A'),
            is_sota=is_sota,
            ic_trend=json.dumps(step1_summary['trend'][-5:]),
            failed_approaches=json.dumps(step1_summary['failed_approaches'][-3:], ensure_ascii=False),
            factor_library_summary=json.dumps(researcher_context.get('factor_library_summary', {}) if researcher_context else {}, ensure_ascii=False)[:1500],
        )
        # 黑名单注入 Step 2 prompt
        if researcher_context and researcher_context.get("factor_blacklist"):
            bl = researcher_context["factor_blacklist"]
            direction_prompt += f"\n\n【因子黑名单】以下因子已被标记不可用，请勿建议添加: {', '.join(bl)}"

        # factor_expand 强制指令：当因子数 < 15 时由 service 层注入方向
        if len(current_factors) < 15:
            direction_prompt += (
                f"\n\n【强制指令】当前因子数={len(current_factors)} < 15，本轮必须执行 factor_expand 方向。"
                f"\n请在 JSON 中设置 direction=\"factor_expand\"，factors_to_remove=[]（不删除任何因子），"
                f"\ntarget_categories 设为当前组合未覆盖的因子类别（避免同类型重复）。"
            )

        try:
            direction_resp = await self._agents.async_call_llm(
                "evolution_factor_agent", direction_system, direction_prompt, step="factor_agent_step2_direction"
            )
            start = direction_resp.find("{")
            end = direction_resp.rfind("}")
            direction = json.loads(direction_resp[start:end+1]) if start != -1 and end != -1 else {}
        except Exception as e:
            raise RuntimeError(f"EvolutionFactorAgent Step 2 LLM failed: {e}") from e

        logger.info(f"EvolutionFactorAgent Step 2 direction: {direction.get('direction')}")

        # ── factor_expand 模式: 只加不删 ──
        if direction.get("direction") == "factor_expand":
            direction["factors_to_remove"] = []
            logger.info("factor_expand 模式: 跳过所有因子删除，只加不删")
        else:
            # ── 重要性保护过滤: 从 factors_to_remove 中移除受保护因子 ──
            original_removals = set(direction.get("factors_to_remove", []))
            protected_removals = original_removals & protected_set
            if protected_removals:
                logger.warning(f"重要性保护: 阻止删除 Top70% 因子 {protected_removals}")
                direction["factors_to_remove"] = [
                    f for f in direction.get("factors_to_remove", []) if f not in protected_set
                ]
            # Max turnover 限制
            if len(direction.get("factors_to_remove", [])) > max_turnover:
                removals = direction["factors_to_remove"]
                removals.sort(key=lambda f: importance_map.get(f, 0.0))
                direction["factors_to_remove"] = removals[:max_turnover]

        # ── Step 3: search_candidate_factors [工具] ──
        # 获取因子黑名单
        factor_blacklist = set()
        if researcher_context and researcher_context.get("factor_blacklist"):
            factor_blacklist = set(researcher_context["factor_blacklist"])

        target_cats = direction.get("target_categories", [])

        # factor_expand: 优先搜索当前因子组合未覆盖的类别，实现类别多样性
        if direction.get("direction") == "factor_expand":
            covered_cats = self._get_factor_categories(current_factors)
            all_cats = list((researcher_context or {}).get("factor_library_summary", {}).keys())
            uncovered = [c for c in all_cats if c not in covered_cats]
            if uncovered:
                target_cats = uncovered
                logger.info(f"factor_expand: 优先搜索未覆盖类别 {uncovered}（已覆盖: {covered_cats}）")
            else:
                logger.info(f"factor_expand: 所有类别已覆盖，全库搜索")
                target_cats = []

        if not target_cats:
            # 从 direction rationale 中推断类别关键词
            rationale = direction.get("rationale", "")
            rationale_lower = rationale.lower()
            category_keywords = {
                "momentum": "ts_momentum", "动量": "ts_momentum",
                "volatility": "ts_volatility", "波动": "ts_volatility",
                "value": "fundamental_value", "价值": "fundamental_value",
                "volume": "ts_volume", "量价": "ts_volume",
                "reversion": "ts_mean_reversion", "反转": "ts_mean_reversion",
            }
            inferred = set()
            for kw, cat in category_keywords.items():
                if kw in rationale_lower:
                    inferred.add(cat)
            target_cats = list(inferred)
            if target_cats:
                logger.info(f"从 rationale 推断 target_categories: {target_cats}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                if target_cats:
                    ph = ",".join(["%s"] * len(target_cats))
                    cur.execute(f"""
                        SELECT c.factor_name, c.category, fr.official_grade, fr.official_score,
                               m.ic_mean AS ind_ic, {_IND_RANK_IC_BEST_ABS_SQL} AS ind_rank_ic_best_abs,
                               c.factor_profile, c.factor_source
                        FROM qe_factor_classification c
                        {_ACTIVE_OFFICIAL_RATING_JOIN_SQL}
                        LEFT JOIN LATERAL (
                            SELECT ic_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name AND eval_window = 'full' AND calc_engine = %s
                            ORDER BY calculated_at DESC LIMIT 1
                        ) m ON TRUE
                        WHERE c.category IN ({ph})
                          AND fc.is_available = TRUE
                        ORDER BY RANDOM()
                        LIMIT 60
                    """, [CALC_ENGINE, *target_cats])
                else:
                    cur.execute(f"""
                        SELECT c.factor_name, c.category, fr.official_grade, fr.official_score,
                               m.ic_mean AS ind_ic, {_IND_RANK_IC_BEST_ABS_SQL} AS ind_rank_ic_best_abs,
                               c.factor_profile, c.factor_source
                        FROM qe_factor_classification c
                        {_ACTIVE_OFFICIAL_RATING_JOIN_SQL}
                        LEFT JOIN LATERAL (
                            SELECT ic_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name AND eval_window = 'full' AND calc_engine = %s
                            ORDER BY calculated_at DESC LIMIT 1
                        ) m ON TRUE
                        WHERE fc.is_available = TRUE
                        ORDER BY RANDOM()
                        LIMIT 60
                    """, (CALC_ENGINE,))
                cols = [desc[0] for desc in cur.description]
                candidates = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 加权抽样偏向高评级（类别过滤后再加权）
        # 未评级/D级因子权重=0.5（取整为1），允许进入候选池但优先级低
        import random
        import math
        grade_weights = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 1}
        weighted = []
        for c in candidates:
            grade = c.get("official_grade") or ""
            w = grade_weights.get(grade, 1)  # 未评级同D级处理，权重=1
            weighted.extend([c] * w)
        if len(weighted) > 40:
            sampled = random.sample(weighted, 40)
            # 去重（加权采样可能重复同一因子）
            seen = set()
            candidates = []
            for c in sampled:
                key = c.get("factor_name")
                if key not in seen:
                    seen.add(key)
                    candidates.append(c)
        else:
            # 去重
            seen = set()
            unique_candidates = []
            for c in weighted:
                if c["factor_name"] not in seen:
                    seen.add(c["factor_name"])
                    unique_candidates.append(c)
            candidates = unique_candidates

        # 黑名单过滤
        if factor_blacklist:
            pre_count = len(candidates)
            candidates = [c for c in candidates if c["factor_name"] not in factor_blacklist]
            if pre_count != len(candidates):
                logger.info(f"黑名单过滤: {pre_count - len(candidates)} 个候选因子被移除")

        logger.info(f"EvolutionFactorAgent Step 3: {len(candidates)} candidates")

        # ── Step 4: screen_candidates [LLM#2] — 从 DB 加载提示词 ──
        candidate_lines = []
        for c in candidates:
            rank_ic_str = (
                f"{float(c['ind_rank_ic_best_abs']):.4f}"
                if c.get("ind_rank_ic_best_abs") is not None else "N/A"
            )
            candidate_lines.append(
                f"{c['factor_name']} | {c['category']} | official={c.get('official_grade') or '未评级'} | "
                f"RankIC_best_abs={rank_ic_str}"
            )

        step4_prompt_data = pm.get_active_prompt_text("evolution_factor_agent", "screen_candidates")
        if not step4_prompt_data:
            raise ValueError("未配置 evolution_factor_agent/screen_candidates 的提示词，拒绝使用兜底策略")
        screen_system = step4_prompt_data["system_prompt"]
        screen_prompt = safe_format(step4_prompt_data["user_prompt_template"],
            direction=direction.get('direction', '替换低效因子'),
            current_factors=json.dumps(current_factors, ensure_ascii=False),
            factors_to_remove=json.dumps(direction.get('factors_to_remove', []), ensure_ascii=False),
            candidate_count=len(candidates),
            candidate_list=chr(10).join(candidate_lines),
        )
        try:
            screen_resp = await self._agents.async_call_llm(
                "evolution_factor_agent", screen_system, screen_prompt, step="factor_agent_step4_screen"
            )
            start = screen_resp.find("{")
            end = screen_resp.rfind("}")
            screen_result = json.loads(screen_resp[start:end+1]) if start != -1 and end != -1 else {}
            screened_names = screen_result.get("screened_factors", [c["factor_name"] for c in candidates[:15]])
        except Exception as e:
            raise RuntimeError(f"EvolutionFactorAgent Step 4 LLM failed: {e}") from e

        logger.info(f"EvolutionFactorAgent Step 4: {len(screened_names)} screened")

        # ── Step 5: fetch_factor_details [工具] ──
        all_factor_names = list(set(current_factors + screened_names))
        factors_to_remove = set(direction.get("factors_to_remove", []))
        factors_to_remove -= protected_set  # 不删除受保护因子
        keep_factors = [f for f in current_factors if f not in factors_to_remove]

        # 获取相关性数据
        correlations = {}
        if researcher_context:
            correlations = researcher_context.get("correlations", {})

        # 获取详细数据
        detail_map = {}
        if all_factor_names:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    ph = ",".join(["%s"] * len(all_factor_names))
                    cur.execute(f"""
                        SELECT c.factor_name, c.category, fr.official_grade, fr.official_score,
                               c.factor_profile,
                               m.ic_mean, m.icir, m.rank_ic_mean, {_IND_RANK_IC_BEST_ABS_SQL} AS ind_rank_ic_best_abs
                        FROM qe_factor_classification c
                        {_ACTIVE_OFFICIAL_RATING_JOIN_SQL}
                        LEFT JOIN LATERAL (
                            SELECT ic_mean, icir, rank_ic_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name AND eval_window = 'full' AND calc_engine = %s
                            ORDER BY calculated_at DESC LIMIT 1
                        ) m ON TRUE
                        WHERE c.factor_name IN ({ph})
                    """, [CALC_ENGINE, *all_factor_names])
                    for row in cur.fetchall():
                        detail_map[row[0]] = {
                            "factor_name": row[0], "category": row[1], "official_grade": row[2],
                            "official_score": row[3], "factor_profile": row[4],
                            "ic_mean": row[5], "icir": row[6], "rank_ic_mean": row[7],
                            "ind_rank_ic_best_abs": row[8],
                        }

        logger.info(f"EvolutionFactorAgent Step 5: {len(detail_map)} factor details fetched")

        # ── Step 6: design_combination [LLM#3] — 从 DB 加载提示词 ──
        # 构建保留因子和新候选因子的详细信息
        keep_lines = []
        for fn in keep_factors:
            d = detail_map.get(fn, {})
            ic = d.get("ic_mean")
            ic_str = f"{float(ic):.4f}" if ic is not None else "N/A"
            imp_val = importance_map.get(fn, 0.0)
            tag = f" [重要性={imp_val:.4f},受保护]" if fn in protected_set else f" [重要性={imp_val:.4f}]"
            keep_lines.append(f"- {fn} | {d.get('category', '?')} | official={d.get('official_grade', '未评级')} | IC={ic_str}{tag}")

        new_lines = []
        for fn in screened_names:
            if fn in set(keep_factors):
                continue
            d = detail_map.get(fn, {})
            ic = d.get("ic_mean")
            ic_str = f"{float(ic):.4f}" if ic is not None else "N/A"
            icir_str = f"{float(d['icir']):.4f}" if d.get("icir") is not None else "N/A"
            new_lines.append(f"- {fn} | {d.get('category', '?')} | official={d.get('official_grade', '未评级')} | IC={ic_str} | ICIR={icir_str}")

        # 相关性警告
        corr_warnings = []
        for key, val in correlations.items():
            if abs(float(val)) > 0.6:
                parts = key.split("_", 1)
                if len(parts) == 2:
                    corr_warnings.append(f"{parts[0]} ↔ {parts[1]}: {float(val):.3f}")

        # 黑名单提示注入
        blacklist_str = ""
        if factor_blacklist:
            blacklist_str = f"\n\n以下因子已被删除或标记不可用，严禁使用: {', '.join(factor_blacklist)}"

        step6_prompt_data = pm.get_active_prompt_text("evolution_factor_agent", "design_combination")
        if not step6_prompt_data:
            raise ValueError("未配置 evolution_factor_agent/design_combination 的提示词，拒绝使用兜底策略")
        design_system = step6_prompt_data["system_prompt"]
        design_prompt = safe_format(step6_prompt_data["user_prompt_template"],
            direction=direction.get('direction', '优化因子组合'),
            direction_rationale=direction.get('rationale', ''),
            keep_count=len(keep_factors),
            keep_factors_detail=chr(10).join(keep_lines) if keep_lines else '无',
            new_count=len(new_lines),
            new_factors_detail=chr(10).join(new_lines) if new_lines else '无',
            correlation_warnings=chr(10).join(corr_warnings) if corr_warnings else '无',
            model_params=json.dumps(current_config.get('model_params', {}), ensure_ascii=False),
        )
        if blacklist_str:
            design_prompt += blacklist_str

        try:
            design_resp = await self._agents.async_call_llm(
                "evolution_factor_agent", design_system, design_prompt, step="factor_agent_step6_design"
            )
            start = design_resp.find("{")
            end = design_resp.rfind("}")
            if start != -1 and end != -1:
                final_config = json.loads(design_resp[start:end+1])
            else:
                raise ValueError("No JSON in response")
        except Exception as e:
            logger.error(f"EvolutionFactorAgent Step 6 LLM failed: {e}")
            raise RuntimeError(
                f"因子组合设计 LLM 调用失败: {e}。"
                f"无法生成因子配置，请检查 LLM 服务状态。"
            ) from e

        logger.info(f"EvolutionFactorAgent Step 6: {len(final_config.get('factor_list', []))} factors in final config")

        # ── Step 6.5: 新增因子相关性预检 ──
        proposed_factors = final_config.get("factor_list", [])
        new_factors = [f for f in proposed_factors if f not in set(keep_factors)]
        if new_factors and keep_factors:
            corr_rejected = []
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 批量获取 factor_name -> catalog_id 映射
                    all_names = list(set(new_factors + keep_factors))
                    ph = ",".join(["%s"] * len(all_names))
                    cur.execute(f"""
                        SELECT DISTINCT ON (factor_name) factor_name, id
                        FROM aistock_factor_catalog
                        WHERE factor_name IN ({ph})
                        ORDER BY factor_name, id
                    """, all_names)
                    id_map = {row[0]: row[1] for row in cur.fetchall()}
                    id_to_name = {v: k for k, v in id_map.items()}
                    keep_ids = [id_map[f] for f in keep_factors if f in id_map]

                    for nf in new_factors:
                        nf_id = id_map.get(nf)
                        if not nf_id or not keep_ids:
                            continue  # 无 catalog 记录的因子默认放行
                        cur.execute("""
                            SELECT factor_a_id, factor_b_id, correlation
                            FROM qe_factor_correlations
                            WHERE (factor_a_id = %s AND factor_b_id = ANY(%s))
                               OR (factor_b_id = %s AND factor_a_id = ANY(%s))
                        """, (nf_id, keep_ids, nf_id, keep_ids))
                        for row in cur.fetchall():
                            corr_val = abs(float(row[2]))
                            if corr_val >= 0.8:
                                # 硬拒绝
                                other_id = row[1] if row[0] == nf_id else row[0]
                                other_name = id_to_name.get(other_id, f"id={other_id}")
                                corr_rejected.append({
                                    "new_factor": nf,
                                    "existing_factor": other_name,
                                    "correlation": float(row[2]),
                                })
                                break
                            elif corr_val >= 0.6:
                                # 记录警告但不拒绝（已在 Step 6 prompt 中提示 LLM）
                                other_id = row[1] if row[0] == nf_id else row[0]
                                other_name = id_to_name.get(other_id, f"id={other_id}")
                                logger.info(f"因子 {nf} 与 {other_name} 相关性 {corr_val:.3f} 在警告区间 [0.6, 0.8)")

            rejected_names = set(r["new_factor"] for r in corr_rejected)
            if rejected_names:
                logger.warning(
                    f"因子相关性预检: {len(rejected_names)} 个新因子因与现有因子相关性 ≥ 0.8 被拒绝: "
                    + ", ".join(f"{r['new_factor']}↔{r['existing_factor']}({r['correlation']:.3f})" for r in corr_rejected)
                )
                final_config["factor_list"] = [f for f in proposed_factors if f not in rejected_names]

        # ── Step 7: 黑名单硬过滤 ──
        if factor_blacklist:
            final_factors = final_config.get("factor_list", [])
            blocked = [f for f in final_factors if f in factor_blacklist]
            if blocked:
                logger.warning(f"因子黑名单硬过滤: 移除 {blocked}")
                final_config["factor_list"] = [f for f in final_factors if f not in factor_blacklist]

        # ── Step 7: validate_config [工具] — F2: 不校验 action_type（由分发层注入） ──
        # 校验因子列表
        if not isinstance(final_config.get("factor_list"), list) or len(final_config["factor_list"]) == 0:
            final_config["factor_list"] = current_factors  # 回退到当前因子

        # Turnover 硬校验: 确保单轮淘汰不超过 max_turnover
        if current_factors:
            final_factors = final_config.get("factor_list", [])
            removed = set(current_factors) - set(final_factors)
            if len(removed) > max_turnover:
                logger.warning(f"Turnover 硬校验: 删除 {len(removed)} 个超限 {max_turnover}，强制保留")
                must_keep = sorted(removed, key=lambda f: -importance_map.get(f, 0.0))[:len(removed) - max_turnover]
                final_config["factor_list"] = final_factors + list(must_keep)

        # 校验模型参数
        if not isinstance(final_config.get("model_params"), dict):
            final_config["model_params"] = current_config.get("model_params", {})

        # 保留基础字段（不含 action_type — 由分发层注入）
        for key in ["model_id", "strategy_id", "data_split", "base_experiment_id"]:
            if key not in final_config and key in current_config:
                final_config[key] = current_config[key]

        logger.info(f"EvolutionFactorAgent Step 7: config validated, {len(final_config.get('factor_list', []))} factors")
        return final_config


class EvolutionModelAgent:
    """
    7步模型编排器 — 与 EvolutionFactorAgent 平行的模型演进编排器。

    Step 1: analyze_model_performance [工具] — 提取当前模型训练诊断、历史表现
    Step 2: decide_model_direction [LLM#1] — 决定 keep/tune/switch
    Step 3: search_candidate_models [工具] — 查询模型目录
    Step 4: evaluate_model_compatibility [LLM#2] — 评估模型与因子兼容性
    Step 5: fetch_model_details [工具] — 获取候选模型详细信息
    Step 6: design_model_config [LLM#3] — 设计最终 model_id + model_params
    Step 7: validate_model_config [工具] — 校验超参合法性
    """

    HYPERPARAM_RANGES = {
        "LGB": {
            "learning_rate": (0.01, 0.3),
            "max_depth": (4, 12),
            "num_leaves": (31, 512),
            "lambda_l1": (0, 500),
            "lambda_l2": (0, 1000),
            "colsample_bytree": (0.5, 1.0),
            "subsample": (0.5, 1.0),
        },
        "XGB": {
            "n_estimators": (100, 2000),
            "max_depth": (3, 12),
            "learning_rate": (0.01, 0.3),
            "subsample": (0.5, 1.0),
            "colsample_bytree": (0.5, 1.0),
            "reg_alpha": (0, 10),
            "reg_lambda": (0, 10),
        },
        "CATBOOST": {
            "iterations": (100, 2000),
            "depth": (3, 10),
            "learning_rate": (0.01, 0.3),
            "l2_leaf_reg": (1, 10),
            "subsample": (0.5, 1.0),
        },
        "LINEAR": {
            "alpha": (0.001, 1.0),
        },
        "PTNN": {
            "n_epochs": (100, 300),
            "lr": (1e-5, 1e-2),
            "batch_size": (2048, 16384),
            "early_stop": (10, 30),
            "weight_decay": (1e-6, 1e-2),
        },
    }

    def __init__(self, agents: Optional["EvolutionAgents"] = None):
        self._agents = agents or EvolutionAgents()

    async def run(
        self,
        analyst_result: 'AnalystResult',
        is_sota: bool,
        current_config: Dict[str, Any],
        evolution_history: Optional[Dict[str, Any]] = None,
        model_context: Optional[Dict[str, Any]] = None,
        factor_context: Optional[Dict[str, Any]] = None,
        sota_context: Optional[Dict[str, Any]] = None,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """执行 7 步模型编排，返回 model_id + model_params 配置。"""
        from ...db.pg_pool import get_conn
        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        hist = evolution_history or {}

        # ── Step 1: analyze_model_performance ──
        current_model_id = current_config.get("model_id", "")
        current_model_params = current_config.get("model_params", {})
        current_metrics = {}
        if hist.get("loops"):
            current_metrics = hist["loops"][-1].get("metrics", {})

        enhanced = current_metrics.get("enhanced_metrics", {})
        training_diag = enhanced.get("training_diagnostics", {})

        # 提取已尝试过的模型
        tried_models = set()
        model_history = []
        for loop in hist.get("loops", []):
            cs = loop.get("config_summary", {})
            mid = cs.get("model_id")
            if mid:
                tried_models.add(mid)
                model_history.append({
                    "loop_index": loop.get("loop_index"),
                    "model_id": mid,
                    "ic": loop.get("metrics", {}).get("IC"),
                    "sharpe": loop.get("metrics", {}).get("sharpe"),
                })

        step1 = {
            "current_model_id": current_model_id,
            "training_diagnostics": training_diag,
            "tried_models": list(tried_models),
            "model_history": model_history,
        }
        logger.info(f"EvolutionModelAgent Step 1: current_model={current_model_id}, tried={len(tried_models)}")

        # ── Step 2: decide_model_direction [LLM#1] ──
        step2_data = pm.get_active_prompt_text("evolution_model_agent", "decide_model_direction")
        if not step2_data:
            raise ValueError("未配置 evolution_model_agent/decide_model_direction 的提示词，拒绝使用兜底策略")
        model_type = self._infer_model_type(current_model_id, current_config)

        step2_system = step2_data["system_prompt"]
        step2_user = safe_format(step2_data["user_prompt_template"],
            best_epoch=training_diag.get("best_epoch", "N/A"),
            convergence_ratio=training_diag.get("convergence_ratio", "N/A"),
            overfit_ratio=training_diag.get("overfit_ratio", "N/A"),
            training_failed=training_diag.get("training_failed", "unknown"),
            current_model_id=current_model_id,
            current_model_type=model_type,
            current_params=json.dumps(current_model_params, ensure_ascii=False),
            model_history=json.dumps(model_history[-5:], ensure_ascii=False),
            tried_models=json.dumps(list(tried_models), ensure_ascii=False),
        )
        # 注入因子组合特征（联合演进时由 Factor Agent 先决定因子组合后传入）
        if factor_context:
            step2_user += (
                f"\n【因子组合特征】总因子数: {factor_context['factor_count']}, "
                f"TS占比: {factor_context['ts_ratio']:.0%}, "
                f"分布: {json.dumps(factor_context['category_distribution'], ensure_ascii=False)}"
                f"\n建议: TS占比>60%优先GRU/LSTM，<40%考虑Tabular"
            )
        # SOTA 回滚上下文注入
        if sota_context and sota_context.get("rollback_applied"):
            cr = sota_context["current_run"]
            sb = sota_context["sota_baseline"]
            step2_user += (
                f"\n\n【SOTA 回滚上下文】本轮非 SOTA，已回滚到 SOTA 配置。"
                f"\n本轮模型: {cr['config'].get('model_id', 'N/A')}, "
                f"IC={cr['metrics'].get('IC', 'N/A')}, AnnRet={cr['metrics'].get('annualized_return', 'N/A')}"
                f"\nSOTA 模型: {sb['config'].get('model_id', 'N/A')}, "
                f"IC={sb['metrics'].get('IC', 'N/A')}, AnnRet={sb['metrics'].get('annualized_return', 'N/A')}"
                f"\n请从 SOTA 基线出发改进，参考本轮失败经验避免重复。"
            )

        try:
            resp2 = await self._agents.async_call_llm("evolution_model_agent", step2_system, step2_user, step="model_agent_step2_analyze")
            start = resp2.find("{")
            end = resp2.rfind("}")
            decision = json.loads(resp2[start:end + 1]) if start != -1 and end != -1 else {}
        except Exception as e:
            raise RuntimeError(f"EvolutionModelAgent Step 2 LLM failed: {e}") from e

        model_decision = decision.get("decision", "tune_hyperparams")
        if training_diag.get("training_failed"):
            best_epoch = training_diag.get("best_epoch", 0)
            if best_epoch == 0:
                # 模型从未改善 → 架构根本问题，硬覆盖为 switch
                model_decision = "switch_model"
                logger.warning("training_failed + best_epoch=0: 架构问题，强制 switch_model")
            else:
                # 有过改善但最终失败 → 可能是超参问题，保留 LLM 决策
                logger.warning(
                    f"training_failed + best_epoch={best_epoch}: 可能是超参问题，"
                    f"LLM 决策 {model_decision} 保留"
                )
        logger.info(f"EvolutionModelAgent Step 2 decision: {model_decision}")

        # ── Step 3: search_candidate_models [工具] ──
        candidate_models = []
        if model_decision == "switch_model":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT model_id, model_name, model_type, ic, annualized_return,
                               is_sota, code_text IS NOT NULL as has_code,
                               model_hyperparameters, model_training_hyperparameters
                        FROM aistock_model_catalog
                        WHERE model_id != %s AND ic IS NOT NULL
                        ORDER BY ic DESC NULLS LAST
                        LIMIT 20
                    """, (current_model_id,))
                    cols = [d[0] for d in cur.description]
                    candidate_models = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 补充 Qlib 内置模型（不需要自定义代码，仅需超参数）
        if model_decision == "switch_model":
            _BUILTIN_MODELS = [
                {
                    "model_id": "__builtin_LGBModel__",
                    "model_name": "LGBModel (Qlib内置, GBDT)",
                    "model_type": "LGB",
                    "ic": None, "has_code": False,
                },
                {
                    "model_id": "__builtin_XGBModel__",
                    "model_name": "XGBModel (Qlib内置, XGBoost)",
                    "model_type": "XGB",
                    "ic": None, "has_code": False,
                },
                {
                    "model_id": "__builtin_CatBoostModel__",
                    "model_name": "CatBoostModel (Qlib内置, CatBoost)",
                    "model_type": "CATBOOST",
                    "ic": None, "has_code": False,
                },
                {
                    "model_id": "__builtin_LinearModel__",
                    "model_name": "LinearModel (Qlib内置, Ridge/Lasso)",
                    "model_type": "LINEAR",
                    "ic": None, "has_code": False,
                },
            ]
            existing_types = {c.get("model_type", "").upper() for c in candidate_models}
            for bm in _BUILTIN_MODELS:
                if bm["model_type"] not in existing_types:
                    candidate_models.append(bm)
        logger.info(f"EvolutionModelAgent Step 3: {len(candidate_models)} candidates (incl. builtins)")

        # ── Step 4: evaluate_model_compatibility [LLM#2] ──
        ranked_models = []
        if model_decision == "switch_model" and candidate_models:
            # 构建因子类别统计
            factor_list = current_config.get("factor_list", [])
            factor_categories = self._get_factor_categories(factor_list)

            step4_data = pm.get_active_prompt_text("evolution_model_agent", "evaluate_compatibility")
            if not step4_data:
                raise ValueError("未配置 evolution_model_agent/evaluate_compatibility 的提示词，拒绝使用兜底策略")
            step4_system = step4_data["system_prompt"]
            step4_user = safe_format(step4_data["user_prompt_template"],
                candidate_models=json.dumps(
                    [{k: v for k, v in c.items() if k != "model_hyperparameters" and k != "model_training_hyperparameters"}
                     for c in candidate_models[:10]], ensure_ascii=False),
                factor_count=len(factor_list),
                factor_categories=json.dumps(factor_categories, ensure_ascii=False),
                model_qe_history=json.dumps(model_history[-5:], ensure_ascii=False),
            )

            try:
                resp4 = await self._agents.async_call_llm("evolution_model_agent", step4_system, step4_user, step="model_agent_step4_select")
                s = resp4.find("{")
                e = resp4.rfind("}")
                if s != -1 and e != -1:
                    ranked_models = json.loads(resp4[s:e + 1]).get("ranked_models", [])
            except Exception as ex:
                raise RuntimeError(f"EvolutionModelAgent Step 4 LLM failed: {ex}") from ex

        logger.info(f"EvolutionModelAgent Step 4: {len(ranked_models)} ranked")

        # ── Step 5: fetch_model_details [工具] ──
        selected_model_id = current_model_id
        if model_decision == "switch_model":
            if ranked_models:
                selected_model_id = ranked_models[0].get("model_id", current_model_id)
            elif candidate_models:
                selected_model_id = candidate_models[0].get("model_id", current_model_id)

        selected_model_type = model_type
        selected_hp = {}
        if selected_model_id != current_model_id:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT model_type, model_hyperparameters, model_training_hyperparameters
                        FROM aistock_model_catalog WHERE model_id = %s
                    """, (selected_model_id,))
                    row = cur.fetchone()
                    if row:
                        selected_model_type = self._classify_model_type(row[0] or "")
                        hp = row[1] or row[2]
                        if hp:
                            selected_hp = hp if isinstance(hp, dict) else json.loads(hp)
        else:
            selected_hp = current_model_params

        logger.info(f"EvolutionModelAgent Step 5: selected={selected_model_id}, type={selected_model_type}")

        # ── Step 6: design_model_config [LLM#3] ──
        hp_ranges = self.HYPERPARAM_RANGES.get(selected_model_type, {})
        step6_data = pm.get_active_prompt_text("evolution_model_agent", "design_model_config")
        if not step6_data:
            raise ValueError("未配置 evolution_model_agent/design_model_config 的提示词，拒绝使用兜底策略")
        step6_system = step6_data["system_prompt"]
        step6_user = safe_format(step6_data["user_prompt_template"],
            selected_model_id=selected_model_id,
            selected_model_type=selected_model_type,
            training_diagnostics=json.dumps(training_diag, ensure_ascii=False),
            current_params=json.dumps(selected_hp, ensure_ascii=False),
            hyperparam_ranges=json.dumps(hp_ranges, ensure_ascii=False),
            model_param_history=json.dumps(model_history[-5:], ensure_ascii=False),
        )

        # ── Optuna 集成：param_tune 时注入贝叶斯建议 ──
        _optuna_trial = None
        if model_decision == "tune_hyperparams" and task_id:
            try:
                from .optuna_optimizer import OptunaHyperparamOptimizer, OPTUNA_AVAILABLE
                if OPTUNA_AVAILABLE:
                    optimizer = OptunaHyperparamOptimizer(task_id, selected_model_type)
                    ask_result = optimizer.ask()
                    if ask_result is not None:
                        _optuna_trial, optuna_params = ask_result
                        step6_user += (
                            f"\n\n【Optuna 贝叶斯建议】以下超参数由 Optuna TPE 基于历史 trial 生成，"
                            f"请参考但可根据诊断信息微调：\n"
                            f"{json.dumps(optuna_params, ensure_ascii=False)}"
                        )
                        logger.info(
                            f"EvolutionModelAgent Step 6: Optuna 建议已注入, "
                            f"trial_number={_optuna_trial.number}"
                        )
                    else:
                        logger.warning(
                            "EvolutionModelAgent Step 6: Optuna ask() 返回 None，回退到纯 LLM 模式"
                        )
                else:
                    logger.warning(
                        "EvolutionModelAgent Step 6: optuna 未安装，回退到纯 LLM 模式"
                    )
            except Exception as e:
                logger.warning(
                    f"EvolutionModelAgent Step 6: Optuna 集成失败: {e}，回退到纯 LLM 模式"
                )

        try:
            resp6 = await self._agents.async_call_llm("evolution_model_agent", step6_system, step6_user, step="model_agent_step6_compose")
            s = resp6.find("{")
            e = resp6.rfind("}")
            if s != -1 and e != -1:
                final_model_config = json.loads(resp6[s:e + 1])
            else:
                raise ValueError("No JSON in response")
        except Exception as ex:
            logger.error(f"EvolutionModelAgent Step 6 LLM failed: {ex}")
            raise RuntimeError(
                f"模型配置设计 LLM 调用失败: {ex}。"
                f"无法生成模型配置，请检查 LLM 服务状态。"
            ) from ex

        logger.info(f"EvolutionModelAgent Step 6: model_id={final_model_config.get('model_id')}")

        # ── Step 7: validate_model_config [工具] ──
        output_model_id = final_model_config.get("model_id", selected_model_id)
        output_params = final_model_config.get("model_params", selected_hp)
        if not isinstance(output_params, dict):
            output_params = selected_hp

        # 超参范围校验
        output_type = self._classify_model_type(output_model_id) if output_model_id != selected_model_id else selected_model_type
        ranges = self.HYPERPARAM_RANGES.get(output_type, {})
        for key, (lo, hi) in ranges.items():
            if key in output_params:
                val = output_params[key]
                if isinstance(val, (int, float)):
                    output_params[key] = max(lo, min(hi, val))

        result = {
            "model_id": output_model_id,
            "model_params": output_params,
            "model_type": output_type,
            "rationale": final_model_config.get("rationale", ""),
        }

        # 保留因子和其他基础字段
        for key in ["factor_list", "strategy_id", "data_split", "base_experiment_id"]:
            if key in current_config:
                result[key] = current_config[key]

        # 附加 Optuna trial 对象（供 process_completed_loop 调用 tell() 反馈结果）
        if _optuna_trial is not None:
            result["_optuna_trial"] = _optuna_trial

        logger.info(f"EvolutionModelAgent Step 7: validated, model={output_model_id}")
        return result

    @staticmethod
    def _infer_model_type(model_id: str, config: Dict) -> str:
        """推断模型类型分类。"""
        mid = (model_id or "").upper()
        if "LGB" in mid or "GBDT" in mid:
            return "LGB"
        if "XGB" in mid or "XGBOOST" in mid:
            return "XGB"
        if "CATBOOST" in mid:
            return "CATBOOST"
        if "LINEAR" in mid or "RIDGE" in mid or "LASSO" in mid:
            return "LINEAR"
        if "PTNN" in mid or "NN" in mid or "LSTM" in mid or "GRU" in mid or "ALSTM" in mid:
            return "PTNN"
        mp = config.get("model_params", {})
        if "n_epochs" in mp or "lr" in mp or "batch_size" in mp:
            return "PTNN"
        if "num_leaves" in mp or ("learning_rate" in mp and "n_estimators" not in mp):
            return "LGB"
        if "n_estimators" in mp and "reg_alpha" in mp:
            return "XGB"
        if "iterations" in mp or "l2_leaf_reg" in mp:
            return "CATBOOST"
        if "estimator" in mp or (len(mp) <= 2 and "alpha" in mp):
            return "LINEAR"
        return "PTNN"

    @staticmethod
    def _classify_model_type(model_type_str: str) -> str:
        """将 DB 中的 model_type 字符串分类。"""
        mt = (model_type_str or "").upper()
        if "LGB" in mt or "GBDT" in mt:
            return "LGB"
        if "XGB" in mt or "XGBOOST" in mt:
            return "XGB"
        if "CATBOOST" in mt:
            return "CATBOOST"
        if "LINEAR" in mt or "RIDGE" in mt or "LASSO" in mt:
            return "LINEAR"
        return "PTNN"

    @staticmethod
    def _get_factor_categories(factor_list: List[str]) -> Dict[str, int]:
        """获取因子类别分布。"""
        from ...db.pg_pool import get_conn
        if not factor_list:
            return {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_list))
                cur.execute(f"""
                    SELECT category, COUNT(*) FROM qe_factor_classification
                    WHERE factor_name IN ({ph}) AND category IS NOT NULL
                    GROUP BY category
                """, factor_list)
                return {row[0]: row[1] for row in cur.fetchall()}


