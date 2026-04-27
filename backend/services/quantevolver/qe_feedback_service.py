"""
QE演进反馈服务
在实验运行完成后生成统一的LLM分析，类似RDAgent的HypothesisFeedback
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn
from .factor_official_evaluation_service import CALC_ENGINE
from .qe_evolution_models import (
    QETrace,
    QELoopRecord,
    QEFeedback,
    build_evolution_context_for_llm,
)

logger = logging.getLogger("aistock.quantevolver.qe_feedback")


class QEFeedbackService:
    """
    QE反馈服务 - 实验运行后生成统一的LLM分析
    
    无论配置是LLM生成还是手工配置，都会在实验运行后生成统一的分析反馈
    """

    # 关键指标列表（用于比较）
    IMPORTANT_METRICS = [
        "IC",
        "ICIR",
        "Rank_IC",
        "Rank_ICIR",
        "excess_return_with_cost_annualized",
        "excess_return_with_cost_IR",
        "excess_return_with_cost_max_drawdown",
    ]

    def __init__(self, llm_client=None):
        """
        Args:
            llm_client: LLM客户端（用于生成分析）
        """
        self.llm_client = llm_client

    def generate_feedback(
        self,
        experiment_id: str,
        experiment_dir: str | Path,
        llm_analysis_text: Optional[str] = None,
    ) -> QEFeedback:
        """
        生成实验反馈
        
        Args:
            experiment_id: 实验ID
            experiment_dir: 实验目录
            llm_analysis_text: 前端已有的LLM分析文本（如果有）
            
        Returns:
            QEFeedback对象
        """
        exp_dir = Path(experiment_dir)

        # 读取轨迹文件
        trace_path = exp_dir / "qe_trace.json"
        trace = None
        if trace_path.exists():
            trace = QETrace.from_json_file(str(trace_path))

        # 读取实验结果
        results = self._read_experiment_results(exp_dir)

        # 获取SOTA比较
        sota_comparison = None
        is_new_sota = False
        if trace and trace.sota_results:
            sota_comparison = self._compare_with_sota(results, trace.sota_results)
            # 判断是否为新的SOTA
            current_return = results.get("performance", {}).get("annualized_return", 0)
            sota_return = trace.sota_results.get("performance", {}).get("annualized_return", 0)
            if current_return and (not sota_return or current_return > sota_return):
                is_new_sota = True

        # 如果有前端传入的LLM分析，解析它
        if llm_analysis_text:
            feedback = self._parse_llm_analysis(
                analysis_text=llm_analysis_text,
                results=results,
                sota_comparison=sota_comparison,
                is_new_sota=is_new_sota,
            )
        else:
            # 生成默认反馈
            feedback = QEFeedback(
                observations=self._generate_observations(results),
                configuration_evaluation=self._evaluate_configuration(results),
                improvement_suggestions=self._generate_suggestions(results, trace),
                risk_warnings=self._generate_risk_warnings(results),
                is_new_sota=is_new_sota,
                sota_comparison=sota_comparison,
                next_focus=self._determine_next_focus(results),
                experiment_id=experiment_id,
            )

        # 更新轨迹文件
        if trace:
            self._update_trace_with_feedback(trace_path, trace, feedback, results, is_new_sota)

        # 保存反馈到数据库
        self._save_feedback_to_db(experiment_id, feedback)

        return feedback

    def _read_experiment_results(self, exp_dir: Path) -> Dict[str, Any]:
        """读取实验结果"""
        results = {
            "signal_quality": {},
            "performance": {},
            "win_rates": {},
            "trading": {},
        }

        # 读取qlib_results.json
        json_path = exp_dir / "qlib_results.json"
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # 提取信号质量
                summary = data.get("summary", {})
                results["signal_quality"] = {
                    "IC": summary.get("IC"),
                    "ICIR": summary.get("ICIR"),
                    "Rank_IC": summary.get("Rank_IC"),
                    "Rank_ICIR": summary.get("Rank_ICIR"),
                }

                # 提取绩效指标
                results["performance"] = {
                    "annualized_return": summary.get("excess_return_with_cost_annualized"),
                    "information_ratio": summary.get("excess_return_with_cost_IR"),
                    "max_drawdown": summary.get("excess_return_with_cost_max_drawdown"),
                    "sharpe_ratio": data.get("daily_win_stats", {}).get("sharpe_ratio"),
                    "calmar_ratio": data.get("calmar_ratio"),
                }

                # 提取胜率
                daily_stats = data.get("daily_win_stats", {})
                stock_stats = data.get("stock_trade_stats", {})
                results["win_rates"] = {
                    "daily_win_rate": daily_stats.get("daily_win_rate"),
                    "weekly_win_rate": daily_stats.get("weekly_win_rate"),
                    "stock_win_rate": stock_stats.get("stock_win_rate"),
                }

                # 提取交易统计
                results["trading"] = {
                    "total_trades": stock_stats.get("total_trades"),
                    "profit_loss_ratio": stock_stats.get("profit_loss_ratio"),
                    "avg_turnover": daily_stats.get("avg_turnover"),
                }

            except Exception as e:
                logger.error(f"读取实验结果失败: {e}")

        return results

    def _compare_with_sota(
        self,
        current_results: Dict[str, Any],
        sota_results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """与SOTA结果比较"""
        comparison = {}

        # 比较信号质量
        for key in ["IC", "ICIR", "Rank_IC", "Rank_ICIR"]:
            current = current_results.get("signal_quality", {}).get(key, 0) or 0
            sota = sota_results.get("signal_quality", {}).get(key, 0) or 0
            comparison[f"delta_{key.lower()}"] = round(current - sota, 6)

        # 比较绩效
        for key in ["annualized_return", "max_drawdown", "sharpe_ratio"]:
            current = current_results.get("performance", {}).get(key, 0) or 0
            sota = sota_results.get("performance", {}).get(key, 0) or 0
            comparison[f"delta_{key}"] = round(current - sota, 6)

        return comparison

    def _parse_llm_analysis(
        self,
        analysis_text: str,
        results: Dict[str, Any],
        sota_comparison: Optional[Dict[str, Any]],
        is_new_sota: bool,
    ) -> QEFeedback:
        """解析前端LLM分析文本为结构化反馈"""
        # 简单解析：将分析文本分段
        sections = self._split_analysis_sections(analysis_text)

        return QEFeedback(
            observations=sections.get("观察", self._generate_observations(results)),
            configuration_evaluation=sections.get("评估", self._evaluate_configuration(results)),
            improvement_suggestions=self._extract_suggestions(sections.get("建议", "")),
            risk_warnings=self._extract_risks(sections.get("风险", "")),
            is_new_sota=is_new_sota,
            sota_comparison=sota_comparison,
            next_focus=self._determine_next_focus(results),
            raw_analysis=analysis_text,
        )

    def _split_analysis_sections(self, text: str) -> Dict[str, str]:
        """分割分析文本为不同段落"""
        sections = {}
        current_section = "其他"
        current_content = []

        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue

            # 检测段落标题
            if any(keyword in line for keyword in ["分析", "观察", "评估", "建议", "风险", "提示"]):
                if current_content:
                    sections[current_section] = "\n".join(current_content)
                current_section = line.replace("##", "").strip()
                current_content = []
            else:
                current_content.append(line)

        if current_content:
            sections[current_section] = "\n".join(current_content)

        return sections

    def _extract_suggestions(self, text: str) -> List[str]:
        """从文本提取建议列表"""
        suggestions = []
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("-") or line.startswith("•") or line.startswith("*"):
                suggestions.append(line.lstrip("-•* ").strip())
            elif line and len(suggestions) == 0:
                suggestions.append(line)
        return suggestions[:5]  # 最多5条

    def _extract_risks(self, text: str) -> List[str]:
        """从文本提取风险提示列表"""
        risks = []
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("-") or line.startswith("•") or line.startswith("*"):
                risks.append(line.lstrip("-•* ").strip())
            elif line and len(risks) == 0:
                risks.append(line)
        return risks[:3]  # 最多3条

    def _generate_observations(self, results: Dict[str, Any]) -> str:
        """生成观察结果"""
        perf = results.get("performance", {})
        signal = results.get("signal_quality", {})

        parts = []

        ic = signal.get("IC")
        if ic is not None:
            if ic > 0.03:
                parts.append(f"IC为{ic:.4f}，预测能力较强")
            elif ic > 0:
                parts.append(f"IC为{ic:.4f}，预测能力较弱")
            else:
                parts.append(f"IC为{ic:.4f}，预测能力为负")

        ann_ret = perf.get("annualized_return")
        if ann_ret is not None:
            if ann_ret > 0.15:
                parts.append(f"年化收益{ann_ret*100:.2f}%，表现优秀")
            elif ann_ret > 0:
                parts.append(f"年化收益{ann_ret*100:.2f}%，表现一般")
            else:
                parts.append(f"年化收益{ann_ret*100:.2f}%，表现不佳")

        max_dd = perf.get("max_drawdown")
        if max_dd is not None:
            if abs(max_dd) > 0.3:
                parts.append(f"最大回撤{abs(max_dd)*100:.2f}%，风险较高")
            elif abs(max_dd) > 0.15:
                parts.append(f"最大回撤{abs(max_dd)*100:.2f}%，风险中等")
            else:
                parts.append(f"最大回撤{abs(max_dd)*100:.2f}%，风险较低")

        return "；".join(parts) if parts else "实验结果已记录"

    def _evaluate_configuration(self, results: Dict[str, Any]) -> str:
        """评估配置效果"""
        perf = results.get("performance", {})
        signal = results.get("signal_quality", {})

        ic = signal.get("IC") or 0
        ann_ret = perf.get("annualized_return") or 0
        max_dd = abs(perf.get("max_drawdown") or 0)

        # 计算综合评分
        score = 0
        if ic > 0.03:
            score += 30
        elif ic > 0.01:
            score += 15
        elif ic > 0:
            score += 5

        if ann_ret > 0.15:
            score += 40
        elif ann_ret > 0.08:
            score += 25
        elif ann_ret > 0:
            score += 10

        if max_dd < 0.1:
            score += 30
        elif max_dd < 0.2:
            score += 20
        elif max_dd < 0.3:
            score += 10

        if score >= 70:
            return f"配置效果优秀（综合评分{score}分），IC、收益、风险控制均表现良好"
        elif score >= 50:
            return f"配置效果中等（综合评分{score}分），部分指标有改进空间"
        else:
            return f"配置效果欠佳（综合评分{score}分），建议重新调整因子或模型组合"

    def _generate_suggestions(
        self,
        results: Dict[str, Any],
        trace: Optional[QETrace],
    ) -> List[str]:
        """生成改进建议"""
        suggestions = []
        perf = results.get("performance", {})
        signal = results.get("signal_quality", {})

        ic = signal.get("IC") or 0
        ann_ret = perf.get("annualized_return") or 0
        max_dd = abs(perf.get("max_drawdown") or 0)

        if ic < 0.02:
            suggestions.append("建议优化因子组合，提升IC值")

        if ann_ret < 0.1:
            suggestions.append("建议调整策略参数或更换模型")

        if max_dd > 0.25:
            suggestions.append("建议增加风险控制参数，降低最大回撤")

        # 基于历史轨迹的建议
        if trace and len(trace.loops) > 1:
            tried_factors = set()
            for loop in trace.loops:
                tried_factors.update(loop.configuration.get("factors", []))

            if len(tried_factors) < 20:
                suggestions.append("建议尝试更多不同的因子组合")
            else:
                suggestions.append("建议聚焦于历史表现较好的因子")

        return suggestions or ["当前配置表现合理，可继续优化细节"]

    def _generate_risk_warnings(self, results: Dict[str, Any]) -> List[str]:
        """生成风险提示"""
        warnings = []
        perf = results.get("performance", {})
        signal = results.get("signal_quality", {})

        ic = signal.get("IC") or 0
        max_dd = abs(perf.get("max_drawdown") or 0)
        ann_ret = perf.get("annualized_return") or 0

        if ic < 0.01:
            warnings.append("模型预测能力较弱，可能无法覆盖交易成本")

        if max_dd > 0.3:
            warnings.append("最大回撤较高，需警惕极端市场风险")

        if ann_ret > 0.2 and max_dd > 0.2:
            warnings.append("高收益伴随高风险，需关注收益稳定性")

        return warnings

    def _determine_next_focus(self, results: Dict[str, Any]) -> List[str]:
        """确定下一步重点"""
        focus = []
        perf = results.get("performance", {})
        signal = results.get("signal_quality", {})

        ic = signal.get("IC") or 0
        max_dd = abs(perf.get("max_drawdown") or 0)
        ann_ret = perf.get("annualized_return") or 0

        if ic < 0.02:
            focus.append("factor")  # 优化因子

        if ann_ret < 0.1 or max_dd > 0.25:
            focus.append("strategy_params")  # 调整策略参数

        if ic < 0.01:
            focus.append("model")  # 更换模型

        return focus or ["fine_tune"]  # 微调

    def _update_trace_with_feedback(
        self,
        trace_path: Path,
        trace: QETrace,
        feedback: QEFeedback,
        results: Dict[str, Any],
        is_new_sota: bool,
    ) -> None:
        """更新轨迹文件，添加反馈"""
        # 更新最后一个LOOP的反馈和结果
        if trace.loops:
            last_loop = trace.loops[-1]
            last_loop.feedback = feedback
            last_loop.results = results
            last_loop.is_new_sota = is_new_sota

        # 如果是新的SOTA，更新SOTA信息
        if is_new_sota:
            trace.sota_loop_id = trace.loops[-1].loop_id if trace.loops else None
            trace.sota_results = results
            if trace.loops:
                trace.sota_config = trace.loops[-1].configuration

        trace.updated_at = datetime.now().isoformat()
        trace.to_json_file(str(trace_path))
        logger.info(f"轨迹文件已更新: {trace_path}")

    def _save_feedback_to_db(self, experiment_id: str, feedback: QEFeedback) -> None:
        """保存反馈到数据库"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_experiments
                        SET llm_feedback = %s,
                            is_sota = %s,
                            updated_at = NOW()
                        WHERE experiment_id = %s
                    """, (
                        json.dumps(feedback.to_dict(), ensure_ascii=False),
                        feedback.is_new_sota,
                        experiment_id,
                    ))
        except Exception as e:
            logger.error(f"保存反馈到数据库失败: {e}")

    def build_next_loop_context(
        self,
        experiment_id: str,
        experiment_dir: str | Path,
    ) -> Dict[str, Any]:
        """
        构建下一轮LOOP的LLM上下文
        
        包含历史轨迹、因子库、策略库、模型库等信息
        """
        exp_dir = Path(experiment_dir)

        # 读取轨迹
        trace_path = exp_dir / "qe_trace.json"
        trace = None
        if trace_path.exists():
            trace = QETrace.from_json_file(str(trace_path))

        # 获取因子目录
        factor_catalog = self._get_factor_catalog()

        # 获取策略目录
        strategy_catalog = self._get_strategy_catalog()

        # 获取模型目录
        model_catalog = self._get_model_catalog()

        if trace:
            return build_evolution_context_for_llm(
                trace=trace,
                factor_catalog=factor_catalog,
                strategy_catalog=strategy_catalog,
                model_catalog=model_catalog,
            )
        else:
            return {
                "error": "No trace found",
                "factor_catalog": factor_catalog,
                "strategy_catalog": strategy_catalog,
                "model_catalog": model_catalog,
            }

    def _get_factor_catalog(self, min_ic: float = 0.0, limit: int = 50) -> List[Dict]:
        """获取因子目录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT c.factor_name, c.description_cn, c.factor_type,
                               m.ic_mean AS ic,
                               m.top_excess_annual_return AS annualized_return,
                               m.top_excess_sharpe AS sharpe
                        FROM aistock_factor_catalog c
                        JOIN LATERAL (
                            SELECT ic_mean, top_excess_annual_return, top_excess_sharpe
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name
                              AND eval_window = 'full'
                              AND calc_engine = %s
                            ORDER BY calculated_at DESC
                            LIMIT 1
                        ) m ON TRUE
                        WHERE m.ic_mean >= %s
                          AND c.is_available = TRUE
                        ORDER BY m.ic_mean DESC
                        LIMIT %s
                    """, (CALC_ENGINE, min_ic, limit))
                    rows = cur.fetchall()
                    return [
                        {
                            "name": row[0],
                            "description": row[1],
                            "type": row[2],
                            "ic": row[3],
                            "annualized_return": row[4],
                            "sharpe": row[5],
                        }
                        for row in rows
                    ]
        except Exception as e:
            logger.error(f"获取因子目录失败: {e}")
            return []

    def _get_strategy_catalog(self) -> List[Dict]:
        """获取策略目录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT strategy_id, display_name, description, strategy_type
                        FROM aistock_strategy_catalog
                        WHERE source_code IS NOT NULL
                    """)
                    rows = cur.fetchall()
                    return [
                        {
                            "id": row[0],
                            "name": row[1],
                            "description": row[2],
                            "type": row[3],
                        }
                        for row in rows
                    ]
        except Exception as e:
            logger.error(f"获取策略目录失败: {e}")
            return []

    def _get_model_catalog(self) -> List[Dict]:
        """获取模型目录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT model_id, display_name, model_type, description
                        FROM aistock_model_catalog
                    """)
                    rows = cur.fetchall()
                    return [
                        {
                            "id": row[0],
                            "name": row[1],
                            "type": row[2],
                            "description": row[3],
                        }
                        for row in rows
                    ]
        except Exception as e:
            logger.error(f"获取模型目录失败: {e}")
            return []
