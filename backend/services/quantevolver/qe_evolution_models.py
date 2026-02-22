"""
QE演进数据模型
定义LLM假设、反馈、轨迹等数据结构，类似RDAgent的Hypothesis和HypothesisFeedback
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
import json


@dataclass
class QEHypothesis:
    """
    QE假设 - 类似RDAgent的Hypothesis
    
    记录配置生成时的假设和理由，无论LLM生成还是手工配置
    """
    # 假设内容
    hypothesis: str  # 假设描述（为什么这样配置）
    
    # 配置来源
    source: str  # "llm_generate" | "manual"
    
    # 配置详情
    configuration: Dict[str, Any] = field(default_factory=dict)
    # - factors: List[str]
    # - model_id: str
    # - strategy_id: str
    # - strategy_params: Dict
    # - custom_params: Dict
    
    # LLM生成时的分析过程（可选）
    reasoning_process: Optional[str] = None
    
    # 时间戳
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # 元数据
    loop_id: Optional[int] = None
    experiment_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QEHypothesis":
        return cls(**data)


@dataclass
class QEFeedback:
    """
    QE反馈 - 类似RDAgent的HypothesisFeedback
    
    实验运行后的统一分析，无论LLM生成还是手工配置
    """
    # 观察结果
    observations: str  # 实验结果观察
    
    # 配置评估
    configuration_evaluation: str  # 配置效果评估
    
    # 改进建议
    improvement_suggestions: List[str] = field(default_factory=list)
    
    # 风险提示
    risk_warnings: List[str] = field(default_factory=list)
    
    # 是否为新的SOTA
    is_new_sota: bool = False
    
    # 与SOTA的比较
    sota_comparison: Optional[Dict[str, Any]] = None
    # {
    #   "delta_annualized_return": +0.03,
    #   "delta_max_drawdown": +0.02,
    #   "delta_ic": +0.003
    # }
    
    # 下一步重点
    next_focus: List[str] = field(default_factory=list)  # ["factor", "model", "strategy_params"]
    
    # 时间戳
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # 元数据
    loop_id: Optional[int] = None
    experiment_id: Optional[str] = None
    
    # 原始分析文本（保留完整内容）
    raw_analysis: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QEFeedback":
        return cls(**data)


@dataclass
class QELoopRecord:
    """
    QE LOOP记录 - 单轮实验的完整记录
    """
    loop_id: int
    
    # 假设（配置生成时）
    hypothesis: Optional[QEHypothesis] = None
    
    # 配置
    configuration: Dict[str, Any] = field(default_factory=dict)
    
    # 实验结果
    results: Dict[str, Any] = field(default_factory=dict)
    # {
    #   "signal_quality": {"IC": 0.023, "ICIR": 0.45, ...},
    #   "performance": {"annualized_return": 0.15, "max_drawdown": -0.08, ...},
    #   "win_rates": {...},
    #   "trading": {...}
    # }
    
    # 反馈（实验运行后）
    feedback: Optional[QEFeedback] = None
    
    # 是否为SOTA
    is_new_sota: bool = False
    
    # 时间戳
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "loop_id": self.loop_id,
            "configuration": self.configuration,
            "results": self.results,
            "is_new_sota": self.is_new_sota,
            "timestamp": self.timestamp,
        }
        if self.hypothesis:
            result["hypothesis"] = self.hypothesis.to_dict()
        if self.feedback:
            result["feedback"] = self.feedback.to_dict()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QELoopRecord":
        hypothesis = None
        if "hypothesis" in data and data["hypothesis"]:
            hypothesis = QEHypothesis.from_dict(data["hypothesis"])
        
        feedback = None
        if "feedback" in data and data["feedback"]:
            feedback = QEFeedback.from_dict(data["feedback"])
        
        return cls(
            loop_id=data["loop_id"],
            hypothesis=hypothesis,
            configuration=data.get("configuration", {}),
            results=data.get("results", {}),
            feedback=feedback,
            is_new_sota=data.get("is_new_sota", False),
            timestamp=data.get("timestamp", datetime.now().isoformat()),
        )


@dataclass
class QETrace:
    """
    QE轨迹 - 所有LOOP的历史记录，类似RDAgent的Trace
    """
    trace_id: str  # 实验ID或演进会话ID
    
    # 创建时间
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # 所有LOOP记录
    loops: List[QELoopRecord] = field(default_factory=list)
    
    # SOTA信息
    sota_loop_id: Optional[int] = None
    sota_config: Optional[Dict[str, Any]] = None
    sota_results: Optional[Dict[str, Any]] = None
    
    def add_loop(self, loop: QELoopRecord) -> None:
        """添加新的LOOP记录"""
        self.loops.append(loop)
        self.updated_at = datetime.now().isoformat()
        
        # 检查是否更新SOTA
        if loop.is_new_sota:
            self.sota_loop_id = loop.loop_id
            self.sota_config = loop.configuration
            self.sota_results = loop.results
    
    def get_sota_loop(self) -> Optional[QELoopRecord]:
        """获取SOTA LOOP"""
        if self.sota_loop_id is None:
            return None
        for loop in self.loops:
            if loop.loop_id == self.sota_loop_id:
                return loop
        return None
    
    def get_previous_loops(self, loop_id: int, limit: int = 5) -> List[QELoopRecord]:
        """获取指定LOOP之前的N个LOOP"""
        result = []
        for loop in reversed(self.loops):
            if loop.loop_id < loop_id:
                result.append(loop)
                if len(result) >= limit:
                    break
        return list(reversed(result))
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "loops": [loop.to_dict() for loop in self.loops],
            "sota_loop_id": self.sota_loop_id,
            "sota_config": self.sota_config,
            "sota_results": self.sota_results,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QETrace":
        trace = cls(
            trace_id=data["trace_id"],
            created_at=data.get("created_at", datetime.now().isoformat()),
            updated_at=data.get("updated_at", datetime.now().isoformat()),
            sota_loop_id=data.get("sota_loop_id"),
            sota_config=data.get("sota_config"),
            sota_results=data.get("sota_results"),
        )
        for loop_data in data.get("loops", []):
            trace.loops.append(QELoopRecord.from_dict(loop_data))
        return trace
    
    def to_json_file(self, path: str) -> None:
        """保存到JSON文件"""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False, default=str)
    
    @classmethod
    def from_json_file(cls, path: str) -> "QETrace":
        """从JSON文件加载"""
        with open(path, "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


# ── 用于LLM Prompt的上下文构建 ──

def build_evolution_context_for_llm(
    trace: QETrace,
    factor_catalog: List[Dict[str, Any]],
    strategy_catalog: List[Dict[str, Any]],
    model_catalog: List[Dict[str, Any]],
    current_loop_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    构建LLM演进分析所需的上下文
    
    Args:
        trace: 历史轨迹
        factor_catalog: 可用因子列表
        strategy_catalog: 可用策略列表
        model_catalog: 可用模型列表
        current_loop_id: 当前LOOP ID（用于获取之前的LOOP）
        
    Returns:
        LLM上下文数据
    """
    # 获取历史LOOP摘要
    loop_summaries = []
    for loop in trace.loops:
        loop_summaries.append({
            "loop_id": loop.loop_id,
            "factors": loop.configuration.get("factors", []),
            "model": loop.configuration.get("model_id"),
            "strategy": loop.configuration.get("strategy_id"),
            "strategy_params": loop.configuration.get("strategy_params", {}),
            "IC": loop.results.get("signal_quality", {}).get("IC"),
            "AnnualizedReturn": loop.results.get("performance", {}).get("annualized_return"),
            "MaxDrawdown": loop.results.get("performance", {}).get("max_drawdown"),
            "SharpeRatio": loop.results.get("performance", {}).get("sharpe_ratio"),
            "is_sota": loop.is_new_sota,
            "hypothesis": loop.hypothesis.hypothesis if loop.hypothesis else None,
            "feedback_summary": loop.feedback.observations[:200] if loop.feedback else None,
        })
    
    return {
        "timestamp": datetime.now().isoformat(),
        "current_loop_id": current_loop_id or (len(trace.loops) + 1),
        
        # 历史轨迹
        "trace": {
            "trace_id": trace.trace_id,
            "total_loops": len(trace.loops),
            "sota_loop_id": trace.sota_loop_id,
            "sota_results": trace.sota_results,
            "loop_summaries": loop_summaries,
        },
        
        # 可用资源
        "available_factors": factor_catalog,
        "available_strategies": strategy_catalog,
        "available_models": model_catalog,
        
        # 演进建议（基于历史分析）
        "evolution_hints": _generate_evolution_hints(trace),
    }


def _generate_evolution_hints(trace: QETrace) -> Dict[str, Any]:
    """
    基于历史轨迹生成演进提示
    """
    hints = {
        "tried_factors": set(),
        "tried_models": set(),
        "tried_strategies": set(),
        "successful_patterns": [],
        "failed_patterns": [],
    }
    
    for loop in trace.loops:
        factors = tuple(sorted(loop.configuration.get("factors", [])))
        model = loop.configuration.get("model_id")
        strategy = loop.configuration.get("strategy_id")
        
        hints["tried_factors"].update(loop.configuration.get("factors", []))
        if model:
            hints["tried_models"].add(model)
        if strategy:
            hints["tried_strategies"].add(strategy)
        
        # 分析成功/失败模式
        if loop.feedback:
            annual_return = loop.results.get("performance", {}).get("annualized_return", 0)
            if annual_return and annual_return > 0.1:  # 年化收益 > 10%
                hints["successful_patterns"].append({
                    "loop_id": loop.loop_id,
                    "factors": factors[:5],  # 只取前5个因子
                    "model": model,
                    "annual_return": annual_return,
                })
            elif annual_return and annual_return < 0:
                hints["failed_patterns"].append({
                    "loop_id": loop.loop_id,
                    "factors": factors[:5],
                    "annual_return": annual_return,
                })
    
    # 转换set为list以便JSON序列化
    hints["tried_factors"] = list(hints["tried_factors"])
    hints["tried_models"] = list(hints["tried_models"])
    hints["tried_strategies"] = list(hints["tried_strategies"])
    
    return hints
