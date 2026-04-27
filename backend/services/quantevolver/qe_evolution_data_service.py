"""
QE演进LLM数据服务
提供因子信息、历史轨迹、配置建议等数据供LLM分析
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from .factor_official_evaluation_service import CALC_ENGINE


class QEEvolutionDataService:
    """QE演进数据服务 - 为LLM提供分析所需的数据"""

    def __init__(self, db_config: dict | None = None):
        self.db_config = db_config or {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "aistock",
            "user": "postgres",
            "password": "lc78080808",
        }
        self._conn = None

    def _get_connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.db_config)
        return self._conn

    def get_factor_catalog_for_llm(self, min_ic: float = 0.0, limit: int = 100) -> list[dict]:
        """
        获取因子目录供LLM选择
        
        Args:
            min_ic: 最小IC阈值过滤
            limit: 返回数量限制
            
        Returns:
            因子列表，包含名称、描述、性能指标等
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT
                c.factor_name,
                c.description_cn,
                c.formula_hint,
                c.factor_type,
                c.data_source,
                c.tags,
                m.ic_mean AS ic,
                m.icir,
                m.top_excess_sharpe AS sharpe,
                m.top_excess_annual_return AS annualized_return,
                m.top_max_drawdown AS max_drawdown,
                m.top_excess_sharpe AS information_ratio,
                c.interface_info,
                c.code_text
            FROM aistock_factor_catalog c
            JOIN LATERAL (
                SELECT ic_mean, icir, top_excess_sharpe,
                       top_excess_annual_return, top_max_drawdown
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
        """
        cursor.execute(query, (CALC_ENGINE, min_ic, limit))
        factors = cursor.fetchall()

        result = []
        for f in factors:
            result.append({
                "name": f["factor_name"],
                "description": f["description_cn"],
                "formula_hint": f["formula_hint"],
                "type": f["factor_type"],
                "data_source": f["data_source"],
                "tags": f["tags"] or [],
                "performance": {
                    "IC": round(f["ic"], 4) if f["ic"] else None,
                    "ICIR": round(f["icir"], 4) if f["icir"] else None,
                    "Sharpe": round(f["sharpe"], 4) if f["sharpe"] else None,
                    "AnnualizedReturn": round(f["annualized_return"], 4) if f["annualized_return"] else None,
                    "MaxDrawdown": round(f["max_drawdown"], 4) if f["max_drawdown"] else None,
                    "InformationRatio": round(f["information_ratio"], 4) if f["information_ratio"] else None,
                },
                "interface": f["interface_info"],
                "has_code": bool(f["code_text"]),
            })

        return result

    def get_strategy_catalog_for_llm(self) -> list[dict]:
        """获取策略目录供LLM选择"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT 
                strategy_id,
                display_name,
                description,
                strategy_type,
                default_kwargs,
                param_schema,
                llm_analysis
            FROM aistock_strategy_catalog
            WHERE source_code IS NOT NULL
        """
        cursor.execute(query)
        strategies = cursor.fetchall()

        result = []
        for s in strategies:
            result.append({
                "id": s["strategy_id"],
                "name": s["display_name"],
                "description": s["description"],
                "type": s["strategy_type"],
                "default_params": s["default_kwargs"] or {},
                "param_schema": s["param_schema"] or {},
                "llm_analysis": s["llm_analysis"],
            })

        return result

    def get_model_catalog_for_llm(self) -> list[dict]:
        """获取模型目录供LLM选择"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT 
                model_id,
                display_name,
                model_type,
                description,
                default_params,
                performance_metrics
            FROM aistock_model_catalog
        """
        cursor.execute(query)
        models = cursor.fetchall()

        result = []
        for m in models:
            result.append({
                "id": m["model_id"],
                "name": m["display_name"],
                "type": m["model_type"],
                "description": m["description"],
                "default_params": m["default_params"] or {},
                "performance": m["performance_metrics"] or {},
            })

        return result

    def get_trace_summary_for_llm(self, task_id: str) -> dict:
        """
        从 DB 获取历史轨迹摘要供LLM分析（替代旧的文件读取方式）。

        Args:
            task_id: 演进任务 ID

        Returns:
            轨迹摘要，包含历史LOOP结果、SOTA配置等
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT loop_id, loop_index, action_type, config_json,
                   metrics_json, agent_analysis, is_sota
            FROM qe_evolution_loops
            WHERE task_id = %s AND status = 'completed'
            ORDER BY loop_index ASC
        """, (task_id,))
        rows = cursor.fetchall()

        loop_summaries = []
        sota_loop_id = None
        sota_config = None
        sota_metrics = None

        for row in rows:
            config = row.get("config_json") or {}
            if isinstance(config, str):
                config = json.loads(config)
            metrics = row.get("metrics_json") or {}
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            analysis = row.get("agent_analysis") or {}
            if isinstance(analysis, str):
                analysis = json.loads(analysis)

            loop_summaries.append({
                "loop_id": row["loop_id"],
                "loop_index": row["loop_index"],
                "action_type": row.get("action_type"),
                "factors": config.get("factor_list", []),
                "model_id": config.get("model_id"),
                "IC": metrics.get("IC"),
                "ICIR": metrics.get("ICIR"),
                "is_sota": row.get("is_sota", False),
                "analyst_report": analysis.get("analyst", ""),
            })

            if row.get("is_sota"):
                sota_loop_id = row["loop_id"]
                sota_config = config
                sota_metrics = metrics

        return {
            "task_id": task_id,
            "total_loops": len(loop_summaries),
            "sota_loop_id": sota_loop_id,
            "sota_config": sota_config,
            "sota_metrics": sota_metrics,
            "loop_summaries": loop_summaries,
        }

    def build_llm_context(
        self,
        task_id: str,
        min_factor_ic: float = 0.0,
        factor_limit: int = 50,
    ) -> dict[str, Any]:
        """
        构建完整的LLM分析上下文（基于 DB，不再依赖文件）。

        Args:
            task_id: 演进任务 ID
            min_factor_ic: 因子IC过滤阈值
            factor_limit: 因子数量限制

        Returns:
            完整的LLM上下文数据
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "factor_catalog": self.get_factor_catalog_for_llm(min_factor_ic, factor_limit),
            "strategy_catalog": self.get_strategy_catalog_for_llm(),
            "model_catalog": self.get_model_catalog_for_llm(),
            "trace_summary": self.get_trace_summary_for_llm(task_id),
        }

    def save_llm_hypothesis(
        self,
        task_id: str,
        loop_id: str,
        hypothesis: dict,
        analysis: str | None = None,
    ) -> None:
        """
        保存LLM生成的假设和分析到 DB（qe_evolution_loops.agent_analysis JSONB）。

        Args:
            task_id: 演进任务 ID
            loop_id: 循环 ID
            hypothesis: LLM生成的假设
            analysis: LLM的分析说明
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 读取现有 agent_analysis
        cursor.execute(
            "SELECT agent_analysis FROM qe_evolution_loops WHERE loop_id = %s",
            (loop_id,),
        )
        row = cursor.fetchone()
        existing = {}
        if row and row.get("agent_analysis"):
            existing = row["agent_analysis"]
            if isinstance(existing, str):
                existing = json.loads(existing)

        # 追加假设和分析
        existing["llm_hypothesis"] = {
            "timestamp": datetime.now().isoformat(),
            "hypothesis": hypothesis,
            "analysis": analysis,
        }

        cursor.execute(
            "UPDATE qe_evolution_loops SET agent_analysis = %s WHERE loop_id = %s",
            (json.dumps(existing, ensure_ascii=False, default=str), loop_id),
        )
        conn.commit()


def main():
    """测试服务"""
    service = QEEvolutionDataService()

    print("=" * 70)
    print("QE演进LLM数据服务测试")
    print("=" * 70)

    # 1. 获取因子目录
    print("\n## 1. 因子目录 (IC > 0.01)")
    factors = service.get_factor_catalog_for_llm(min_ic=0.01, limit=10)
    for f in factors:
        print(f"  - {f['name']}: IC={f['performance']['IC']}, {f['description'][:30]}...")

    # 2. 获取策略目录
    print("\n## 2. 策略目录")
    strategies = service.get_strategy_catalog_for_llm()
    for s in strategies:
        print(f"  - {s['id']}: {s['name']}")

    # 3. 显示上下文摘要（需要传入 task_id）
    test_task_id = "Evo_test"
    context = service.build_llm_context(test_task_id, min_factor_ic=0.0, factor_limit=30)
    print(f"\n## 3. 上下文摘要 (task_id={test_task_id})")
    print(f"  因子数量: {len(context['factor_catalog'])}")
    print(f"  策略数量: {len(context['strategy_catalog'])}")
    print(f"  模型数量: {len(context['model_catalog'])}")
    print(f"  历史LOOP: {context['trace_summary']['total_loops']}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
