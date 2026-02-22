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
                factor_name,
                description_cn,
                formula_hint,
                factor_type,
                data_source,
                tags,
                ic,
                icir,
                sharpe,
                annualized_return,
                max_drawdown,
                information_ratio,
                interface_info,
                code_text
            FROM aistock_factor_catalog
            WHERE ic IS NOT NULL AND ic >= %s
            ORDER BY ic DESC
            LIMIT %s
        """
        cursor.execute(query, (min_ic, limit))
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

    def get_trace_summary_for_llm(self, trace_path: str | Path) -> dict:
        """
        获取历史轨迹摘要供LLM分析
        
        Args:
            trace_path: qe_trace.json文件路径
            
        Returns:
            轨迹摘要，包含历史LOOP结果、SOTA配置等
        """
        trace_file = Path(trace_path)
        if not trace_file.exists():
            return {"error": "Trace file not found", "loops": [], "sota": None}

        with open(trace_file, encoding="utf-8") as f:
            trace = json.load(f)

        # 构建精简的历史摘要
        loop_summaries = []
        for loop in trace.get("loops", []):
            loop_summaries.append({
                "loop_id": loop.get("loop_id"),
                "factors": loop.get("configuration", {}).get("factors", []),
                "model": loop.get("configuration", {}).get("model", {}).get("class"),
                "strategy_params": loop.get("configuration", {}).get("strategy", {}).get("kwargs", {}),
                "IC": loop.get("results", {}).get("signal_quality", {}).get("IC"),
                "AnnualizedReturn": loop.get("results", {}).get("performance", {}).get("annualized_return"),
                "MaxDrawdown": loop.get("results", {}).get("performance", {}).get("max_drawdown"),
                "SharpeRatio": loop.get("results", {}).get("performance", {}).get("sharpe_ratio"),
                "is_sota": loop.get("is_new_sota", False),
            })

        return {
            "trace_id": trace.get("trace_id"),
            "total_loops": len(loop_summaries),
            "sota_loop_id": trace.get("sota_loop_id"),
            "sota_config": trace.get("sota_config"),
            "sota_results": trace.get("sota_results"),
            "loop_summaries": loop_summaries,
            "llm_hypotheses": trace.get("llm_hypotheses", []),
            "llm_analyses": trace.get("llm_analyses", []),
        }

    def build_llm_context(
        self,
        trace_path: str | Path,
        min_factor_ic: float = 0.0,
        factor_limit: int = 50,
    ) -> dict[str, Any]:
        """
        构建完整的LLM分析上下文
        
        Args:
            trace_path: 历史轨迹文件路径
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
            "trace_summary": self.get_trace_summary_for_llm(trace_path),
        }

    def export_llm_context_to_json(
        self,
        output_path: str | Path,
        trace_path: str | Path,
        min_factor_ic: float = 0.0,
        factor_limit: int = 50,
    ) -> Path:
        """
        导出LLM上下文到JSON文件
        
        Args:
            output_path: 输出文件路径
            trace_path: 历史轨迹文件路径
            min_factor_ic: 因子IC过滤阈值
            factor_limit: 因子数量限制
            
        Returns:
            输出文件路径
        """
        context = self.build_llm_context(trace_path, min_factor_ic, factor_limit)
        output_file = Path(output_path)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(context, f, indent=2, ensure_ascii=False, default=str)

        return output_file

    def save_llm_hypothesis(
        self,
        trace_path: str | Path,
        hypothesis: dict,
        analysis: str | None = None,
    ) -> None:
        """
        保存LLM生成的假设和分析到轨迹
        
        Args:
            trace_path: 轨迹文件路径
            hypothesis: LLM生成的假设
            analysis: LLM的分析说明
        """
        trace_file = Path(trace_path)
        if not trace_file.exists():
            return

        with open(trace_file, encoding="utf-8") as f:
            trace = json.load(f)

        hypothesis_record = {
            "timestamp": datetime.now().isoformat(),
            "hypothesis": hypothesis,
            "analysis": analysis,
        }

        trace.setdefault("llm_hypotheses", []).append(hypothesis_record)
        if analysis:
            trace.setdefault("llm_analyses", []).append({
                "timestamp": datetime.now().isoformat(),
                "analysis": analysis,
            })

        trace["updated_at"] = datetime.now().isoformat()

        with open(trace_file, "w", encoding="utf-8") as f:
            json.dump(trace, f, indent=2, ensure_ascii=False, default=str)


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

    # 3. 导出完整上下文
    trace_path = Path(__file__).parent.parent / "RD-Agent-main/qe_workspace/qe_exp_3b80e822/qe_trace.json"
    output_path = Path(__file__).parent.parent / "RD-Agent-main/qe_workspace/qe_exp_3b80e822/llm_context.json"

    print(f"\n## 3. 导出LLM上下文")
    print(f"  轨迹文件: {trace_path}")
    print(f"  输出文件: {output_path}")

    result_path = service.export_llm_context_to_json(output_path, trace_path, min_factor_ic=0.0, factor_limit=30)
    print(f"  已导出: {result_path}")

    # 4. 显示上下文摘要
    context = service.build_llm_context(trace_path, min_factor_ic=0.0, factor_limit=30)
    print(f"\n## 4. 上下文摘要")
    print(f"  因子数量: {len(context['factor_catalog'])}")
    print(f"  策略数量: {len(context['strategy_catalog'])}")
    print(f"  模型数量: {len(context['model_catalog'])}")
    print(f"  历史LOOP: {context['trace_summary']['total_loops']}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
