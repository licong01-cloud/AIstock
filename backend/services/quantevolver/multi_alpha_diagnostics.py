"""
Multi-Alpha Diagnostics Service (v2.0).

Provides rich diagnostic information for guiding manual Multi-Alpha evolution:
  - Group performance matrix
  - Group-pair prediction correlations
  - Bottleneck identification (rules-based, deterministic)
  - Action recommendations (linked to evolve wizard)
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Any

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.multi_alpha_diagnostics")


# ── Data Classes ──────────────────────────────────────────────────────

@dataclass
class GroupMetrics:
    group_name: str
    factor_count: int
    model_id: str
    dataset_type: str
    compute_resource: str
    group_ic: float | None = None
    group_icir: float | None = None
    group_sharpe: float | None = None
    meta_weight: float | None = None
    status: str = "pending"
    assigned_node_id: str | None = None
    prediction_path: str | None = None
    factor_names: list[str] = field(default_factory=list)


@dataclass
class Bottleneck:
    rule_id: str
    severity: str  # "high" | "medium" | "low"
    message: str
    affected_groups: list[str]
    recommendation: str
    action_type: str  # "remove_group" | "merge_groups" | "switch_model" | "add_factors" | "tune_meta"
    action_params: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiagnosticsReport:
    experiment_id: str
    groups: list[GroupMetrics]
    meta_method: str | None
    execution_mode: str | None
    correlations: dict[str, float]  # "{group_a}|{group_b}" → corr value
    bottlenecks: list[Bottleneck]
    recommendations: list[Bottleneck]  # same structure, prioritized
    combined_ic: float | None = None


# ── Service ───────────────────────────────────────────────────────────

class MultiAlphaDiagnostics:
    """Analyzes completed Multi-Alpha experiments for guided evolution."""

    def analyze(self, experiment_id: str) -> dict[str, Any]:
        """Full diagnostic analysis for a Multi-Alpha experiment."""
        groups = self._load_groups(experiment_id)
        if not groups:
            return {"ok": False, "error": f"No groups found for experiment {experiment_id}"}

        meta_info = self._load_meta_info(experiment_id)
        correlations = self._load_correlations(experiment_id)
        bottlenecks = self.identify_bottlenecks(groups, correlations)
        recommendations = self._prioritize_recommendations(bottlenecks)

        report = DiagnosticsReport(
            experiment_id=experiment_id,
            groups=groups,
            meta_method=meta_info.get("method"),
            execution_mode=meta_info.get("execution_mode"),
            correlations=correlations,
            bottlenecks=bottlenecks,
            recommendations=recommendations,
            combined_ic=meta_info.get("combined_ic"),
        )
        return {"ok": True, "diagnostics": asdict(report)}

    def compute_group_correlations(self, experiment_id: str) -> dict[str, Any]:
        """Return cached group-pair correlations."""
        correlations = self._load_correlations(experiment_id)
        return {"ok": True, "experiment_id": experiment_id, "correlations": correlations}

    def get_recommendations(self, experiment_id: str) -> dict[str, Any]:
        """Return prioritized action recommendations."""
        groups = self._load_groups(experiment_id)
        correlations = self._load_correlations(experiment_id)
        bottlenecks = self.identify_bottlenecks(groups, correlations)
        recommendations = self._prioritize_recommendations(bottlenecks)
        return {
            "ok": True,
            "experiment_id": experiment_id,
            "recommendations": [asdict(r) for r in recommendations],
        }

    # ── Bottleneck Rules Engine ────────────────────────────────────

    def identify_bottlenecks(
        self,
        groups: list[GroupMetrics],
        correlations: dict[str, float],
    ) -> list[Bottleneck]:
        """Deterministic rules-based bottleneck identification."""
        bottlenecks: list[Bottleneck] = []

        completed_groups = [g for g in groups if g.status == "completed"]
        if not completed_groups:
            return bottlenecks

        # Rule 1: 无效组 (weight < 0.05 或 IC < 0.01)
        for g in completed_groups:
            if g.meta_weight is not None and g.meta_weight < 0.05:
                bottlenecks.append(Bottleneck(
                    rule_id="zero_weight",
                    severity="high",
                    message=f"组 {g.group_name} 的 Meta 权重为 {g.meta_weight:.3f} (接近0)，"
                            f"IC={g.group_ic:.4f if g.group_ic else 'N/A'}，对合成信号无贡献。",
                    affected_groups=[g.group_name],
                    recommendation=f"建议删除 {g.group_name} 组，或尝试更换模型 (当前: {g.model_id})",
                    action_type="remove_group",
                    action_params={"target_group": g.group_name},
                ))
            elif g.group_ic is not None and abs(g.group_ic) < 0.01:
                bottlenecks.append(Bottleneck(
                    rule_id="low_ic",
                    severity="medium",
                    message=f"组 {g.group_name} IC={g.group_ic:.4f} 低于 0.01 阈值。",
                    affected_groups=[g.group_name],
                    recommendation=f"建议为 {g.group_name} 增加更多高IC因子或更换模型",
                    action_type="switch_model",
                    action_params={"target_group": g.group_name, "current_model": g.model_id},
                ))

        # Rule 2: 高相关组对 (corr > 0.7)
        for pair_key, corr_val in correlations.items():
            if abs(corr_val) > 0.7:
                parts = pair_key.split("|")
                if len(parts) == 2:
                    bottlenecks.append(Bottleneck(
                        rule_id="high_correlation",
                        severity="medium",
                        message=f"组 {parts[0]} 和 {parts[1]} 的预测相关性为 {corr_val:.3f} (>0.7)，"
                                f"信号高度重叠，造成冗余。",
                        affected_groups=list(parts),
                        recommendation=f"建议合并 {parts[0]} 和 {parts[1]}，或从其中一个移除高相关因子",
                        action_type="merge_groups",
                        action_params={"group_a": parts[0], "group_b": parts[1]},
                    ))

        # Rule 3: 高 ICIR 低权重 (ICIR top-2 但 weight bottom-2)
        if len(completed_groups) >= 4:
            by_icir = sorted(
                [g for g in completed_groups if g.group_icir is not None],
                key=lambda g: abs(g.group_icir or 0),
                reverse=True,
            )
            by_weight = sorted(
                [g for g in completed_groups if g.meta_weight is not None],
                key=lambda g: g.meta_weight or 0,
            )
            if by_icir and by_weight:
                top_icir_names = {g.group_name for g in by_icir[:2]}
                low_weight_names = {g.group_name for g in by_weight[:2]}
                overlap = top_icir_names & low_weight_names
                for name in overlap:
                    g = next(g for g in completed_groups if g.group_name == name)
                    icir_val = g.group_icir if g.group_icir is not None else 0.0
                    weight_val = g.meta_weight if g.meta_weight is not None else 0.0
                    bottlenecks.append(Bottleneck(
                        rule_id="icir_weight_mismatch",
                        severity="low",
                        message=f"组 {name} ICIR={icir_val:.3f} (top-2) 但权重仅 {weight_val:.3f} (bottom-2)。"
                                f"Meta-Model 可能欠拟合此组信号。",
                        affected_groups=[name],
                        recommendation="尝试增加 Meta-Model 的 lookback_days 或切换到 OLS/Stacking 方法",
                        action_type="tune_meta",
                        action_params={"target_group": name},
                    ))

        # Rule 4: 单组垄断 (weight > 60%)
        for g in completed_groups:
            if g.meta_weight is not None and g.meta_weight > 0.6 and len(completed_groups) >= 3:
                bottlenecks.append(Bottleneck(
                    rule_id="weight_monopoly",
                    severity="medium",
                    message=f"组 {g.group_name} 的 Meta 权重为 {g.meta_weight:.1%}，远超其他组。"
                            f"多Alpha的分散化效果有限。",
                    affected_groups=[g.group_name],
                    recommendation="建议增强其他组的因子或新增独立数据源的组",
                    action_type="add_factors",
                    action_params={"dominant_group": g.group_name},
                ))

        # Rule 5: IC 短长期衰减过快 (需要 factor_metrics 数据，简化检查)
        # 暂不实现 — 需要 per-group multi-horizon IC 数据

        return bottlenecks

    # ── Private Helpers ────────────────────────────────────────────

    def _load_groups(self, experiment_id: str) -> list[GroupMetrics]:
        """Load group records from DB."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT group_name, factor_names, model_id, dataset_type,
                               compute_resource, group_ic, group_icir, group_sharpe,
                               meta_weight, status, assigned_node_id, prediction_path
                        FROM qe_multi_alpha_groups
                        WHERE parent_experiment_id = %s
                        ORDER BY group_name
                    """, (experiment_id,))
                    rows = cur.fetchall()
                    return [
                        GroupMetrics(
                            group_name=r[0],
                            factor_names=r[1] if isinstance(r[1], list) else json.loads(r[1]) if r[1] else [],
                            model_id=r[2],
                            dataset_type=r[3] or "DatasetH",
                            compute_resource=r[4] or "cpu",
                            group_ic=r[5],
                            group_icir=r[6],
                            group_sharpe=r[7],
                            meta_weight=r[8],
                            status=r[9] or "pending",
                            assigned_node_id=r[10],
                            prediction_path=r[11],
                            factor_count=len(r[1] if isinstance(r[1], list) else json.loads(r[1]) if r[1] else []),
                        )
                        for r in rows
                    ]
        except Exception as e:
            logger.error(f"Failed to load groups for {experiment_id}: {e}")
            return []

    def _load_meta_info(self, experiment_id: str) -> dict[str, Any]:
        """Load meta-model info from qe_experiments + qe_meta_model_weights."""
        info: dict[str, Any] = {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # From experiment record
                    cur.execute(
                        "SELECT multi_alpha_config FROM qe_experiments WHERE experiment_id = %s",
                        (experiment_id,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        mac = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                        info["method"] = mac.get("meta_model", {}).get("method", "ic_weighted")
                        info["execution_mode"] = mac.get("execution_mode", "serial")

                    # Latest meta weights
                    cur.execute("""
                        SELECT method, combined_ic
                        FROM qe_meta_model_weights
                        WHERE experiment_id = %s
                        ORDER BY as_of_date DESC LIMIT 1
                    """, (experiment_id,))
                    wrow = cur.fetchone()
                    if wrow:
                        info["method"] = wrow[0]
                        info["combined_ic"] = wrow[1]
        except Exception as e:
            logger.error(f"Failed to load meta info: {e}")
        return info

    def _load_correlations(self, experiment_id: str) -> dict[str, float]:
        """Load cached group-pair correlations."""
        corrs: dict[str, float] = {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT group_a, group_b, correlation
                        FROM qe_group_prediction_correlations
                        WHERE experiment_id = %s
                    """, (experiment_id,))
                    for r in cur.fetchall():
                        corrs[f"{r[0]}|{r[1]}"] = r[2]
        except Exception as e:
            logger.error(f"Failed to load correlations: {e}")
        return corrs

    def _prioritize_recommendations(self, bottlenecks: list[Bottleneck]) -> list[Bottleneck]:
        """Sort bottlenecks by severity (high → medium → low)."""
        severity_order = {"high": 0, "medium": 1, "low": 2}
        return sorted(bottlenecks, key=lambda b: severity_order.get(b.severity, 3))
