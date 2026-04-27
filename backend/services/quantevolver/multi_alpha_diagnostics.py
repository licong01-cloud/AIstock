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
from dataclasses import asdict, dataclass, field
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
    combined_vs_groups: dict[str, Any] = field(default_factory=dict)
    portfolio_diagnostics: dict[str, Any] = field(default_factory=dict)
    diversification: dict[str, Any] = field(default_factory=dict)
    group_diagnostics: list[dict[str, Any]] = field(default_factory=list)
    data_availability: dict[str, Any] = field(default_factory=dict)
    ic_quality: dict[str, Any] = field(default_factory=dict)
    optimization_guidance: list[dict[str, Any]] = field(default_factory=list)


# ── Service ───────────────────────────────────────────────────────────

class MultiAlphaDiagnostics:
    """Analyzes completed Multi-Alpha experiments for guided evolution."""

    def analyze(self, experiment_id: str) -> dict[str, Any]:
        """Full diagnostic analysis for a Multi-Alpha experiment."""
        groups, meta_info, correlations, analysis = self._load_analysis_inputs(experiment_id)
        if not groups:
            return {"ok": False, "error": f"No groups found for experiment {experiment_id}"}

        bottlenecks = self.identify_bottlenecks(groups, correlations)
        recommendations = self._prioritize_recommendations(bottlenecks)
        analysis = self._ensure_analysis(analysis, groups, meta_info, correlations)

        report = DiagnosticsReport(
            experiment_id=experiment_id,
            groups=groups,
            meta_method=meta_info.get("method"),
            execution_mode=meta_info.get("execution_mode"),
            correlations=correlations,
            bottlenecks=bottlenecks,
            recommendations=recommendations,
            combined_ic=meta_info.get("combined_ic"),
            combined_vs_groups=analysis.get("combined_vs_groups") or {},
            portfolio_diagnostics=analysis.get("portfolio_diagnostics") or {},
            diversification=analysis.get("diversification") or {},
            group_diagnostics=analysis.get("group_diagnostics") or [],
            data_availability=analysis.get("data_availability") or {},
            ic_quality=analysis.get("ic_quality") or {},
            optimization_guidance=analysis.get("optimization_guidance") or [],
        )
        return {"ok": True, "diagnostics": asdict(report)}

    def compute_group_correlations(self, experiment_id: str) -> dict[str, Any]:
        """Return cached group-pair correlations."""
        _, _, correlations, _ = self._load_analysis_inputs(experiment_id)
        return {"ok": True, "experiment_id": experiment_id, "correlations": correlations}

    def get_recommendations(self, experiment_id: str) -> dict[str, Any]:
        """Return prioritized action recommendations."""
        groups, meta_info, correlations, analysis = self._load_analysis_inputs(experiment_id)
        bottlenecks = self.identify_bottlenecks(groups, correlations)
        recommendations = self._prioritize_recommendations(bottlenecks)
        analysis = self._ensure_analysis(analysis, groups, meta_info, correlations)
        return {
            "ok": True,
            "experiment_id": experiment_id,
            "recommendations": [asdict(r) for r in recommendations],
            "optimization_guidance": analysis.get("optimization_guidance") or [],
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
            ic_text = f"{g.group_ic:.4f}" if g.group_ic is not None else "N/A"
            if g.meta_weight is not None and g.meta_weight < 0.05:
                bottlenecks.append(Bottleneck(
                    rule_id="zero_weight",
                    severity="high",
                    message=(
                        f"组 {g.group_name} 的 Meta 权重为 {g.meta_weight:.3f} (接近0)，"
                        f"IC={ic_text}，对合成信号无贡献。"
                    ),
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
                        message=(
                            f"组 {parts[0]} 和 {parts[1]} 的预测相关性为 {corr_val:.3f} (>0.7)，"
                            f"信号高度重叠，造成冗余。"
                        ),
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
                        message=(
                            f"组 {name} ICIR={icir_val:.3f} (top-2) 但权重仅 {weight_val:.3f} (bottom-2)。"
                            f"Meta-Model 可能欠拟合此组信号。"
                        ),
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
                    message=(
                        f"组 {g.group_name} 的 Meta 权重为 {g.meta_weight:.1%}，远超其他组。"
                        f"多Alpha的分散化效果有限。"
                    ),
                    affected_groups=[g.group_name],
                    recommendation="建议增强其他组的因子或新增独立数据源的组",
                    action_type="add_factors",
                    action_params={"dominant_group": g.group_name},
                ))

        return bottlenecks

    # ── Private Helpers ────────────────────────────────────────────

    def _load_analysis_inputs(
        self,
        experiment_id: str,
    ) -> tuple[list[GroupMetrics], dict[str, Any], dict[str, float], dict[str, Any]]:
        unified = self._load_unified_detail(experiment_id)
        groups = self._merge_groups_with_unified(self._load_groups(experiment_id), unified)
        if not groups:
            groups = self._build_groups_from_unified(unified)

        meta_info = self._load_meta_info(experiment_id)
        if unified.get("meta_method") and not meta_info.get("method"):
            meta_info["method"] = unified.get("meta_method")
        if unified.get("execution_mode") and not meta_info.get("execution_mode"):
            meta_info["execution_mode"] = unified.get("execution_mode")
        if unified.get("combined_ic") is not None and meta_info.get("combined_ic") is None:
            meta_info["combined_ic"] = unified.get("combined_ic")

        correlations = self._load_correlations(experiment_id)
        if not correlations:
            correlations = unified.get("correlations") or {}

        return groups, meta_info, correlations, unified.get("analysis") or {}

    def _load_groups(self, experiment_id: str) -> list[GroupMetrics]:
        """Load group records from DB."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT group_name, factor_names, model_id, dataset_type,
                               compute_resource, group_ic, group_icir, group_sharpe,
                               meta_weight, status, assigned_node_id, prediction_path
                        FROM qe_multi_alpha_groups
                        WHERE parent_experiment_id = %s
                        ORDER BY group_name
                        """,
                        (experiment_id,),
                    )
                    rows = cur.fetchall()
                    return [
                        GroupMetrics(
                            group_name=r[0],
                            factor_names=self._parse_factor_names(r[1]),
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
                            factor_count=len(self._parse_factor_names(r[1])),
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
                    cur.execute(
                        "SELECT multi_alpha_config FROM qe_experiments WHERE experiment_id = %s",
                        (experiment_id,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        mac = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                        info["method"] = mac.get("meta_model", {}).get("method", "ic_weighted")
                        info["execution_mode"] = mac.get("execution_mode", "serial")

                    cur.execute(
                        """
                        SELECT method, combined_ic
                        FROM qe_meta_model_weights
                        WHERE experiment_id = %s
                        ORDER BY as_of_date DESC LIMIT 1
                        """,
                        (experiment_id,),
                    )
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
                    cur.execute(
                        """
                        SELECT group_a, group_b, correlation
                        FROM qe_group_prediction_correlations
                        WHERE experiment_id = %s
                        """,
                        (experiment_id,),
                    )
                    for r in cur.fetchall():
                        corrs[f"{r[0]}|{r[1]}"] = r[2]
        except Exception as e:
            logger.error(f"Failed to load correlations: {e}")
        return corrs

    def _load_unified_detail(self, experiment_id: str) -> dict[str, Any]:
        """Load multi_alpha_detail fallback from qe_experiments.result_metrics."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status, result_metrics, multi_alpha_config FROM qe_experiments WHERE experiment_id = %s",
                        (experiment_id,),
                    )
                    row = cur.fetchone()
        except Exception as e:
            logger.error(f"Failed to load unified detail for {experiment_id}: {e}")
            return {}

        if not row:
            return {}

        status, result_metrics_raw, multi_alpha_config_raw = row
        result_metrics = self._parse_jsonish(result_metrics_raw) or {}
        multi_alpha_config = self._parse_jsonish(multi_alpha_config_raw) or {}
        detail = result_metrics.get("multi_alpha_detail") or {}
        nested_enhanced = result_metrics.get("enhanced_metrics") or {}
        if not isinstance(nested_enhanced, dict):
            nested_enhanced = {}
        analysis = result_metrics.get("multi_alpha_analysis") or nested_enhanced.get("multi_alpha_analysis") or {}
        if not isinstance(analysis, dict):
            analysis = {}
        return {
            "status": status,
            "multi_detail": detail,
            "meta_method": detail.get("meta_method") or multi_alpha_config.get("meta_model", {}).get("method"),
            "execution_mode": multi_alpha_config.get("execution_mode"),
            "combined_ic": detail.get("combined_ic") if detail else result_metrics.get("IC"),
            "correlations": detail.get("group_correlations") or {},
            "analysis": analysis,
        }

    def _build_groups_from_unified(self, unified: dict[str, Any]) -> list[GroupMetrics]:
        detail = unified.get("multi_detail") or {}
        status = unified.get("status") or "pending"
        groups: list[GroupMetrics] = []
        for group in detail.get("group_results") or []:
            if not isinstance(group, dict) or not group.get("group_name"):
                continue
            factor_names = self._parse_factor_names(group.get("factor_names"))
            factor_count = group.get("factor_count")
            if factor_count is None:
                factor_count = len(factor_names)
            groups.append(
                GroupMetrics(
                    group_name=group["group_name"],
                    factor_count=int(factor_count or 0),
                    factor_names=factor_names,
                    model_id=group.get("model_id") or "-",
                    dataset_type=group.get("dataset_type") or "DatasetH",
                    compute_resource=group.get("compute_resource") or "cpu",
                    group_ic=group.get("ic"),
                    group_icir=group.get("icir"),
                    group_sharpe=group.get("sharpe"),
                    meta_weight=group.get("meta_weight"),
                    status=group.get("status") or ("completed" if status == "completed" else status),
                    assigned_node_id=group.get("assigned_node_id"),
                    prediction_path=group.get("prediction_path"),
                )
            )
        return groups

    def _merge_groups_with_unified(
        self,
        groups: list[GroupMetrics],
        unified: dict[str, Any],
    ) -> list[GroupMetrics]:
        if not groups:
            return []

        detail = unified.get("multi_detail") or {}
        detail_by_name = {
            g.get("group_name"): g
            for g in detail.get("group_results") or []
            if isinstance(g, dict) and g.get("group_name")
        }
        exp_status = unified.get("status")

        merged: list[GroupMetrics] = []
        for group in groups:
            detail_group = detail_by_name.get(group.group_name) or {}
            if group.group_ic is None and detail_group.get("ic") is not None:
                group.group_ic = detail_group.get("ic")
            if group.group_icir is None and detail_group.get("icir") is not None:
                group.group_icir = detail_group.get("icir")
            if group.group_sharpe is None and detail_group.get("sharpe") is not None:
                group.group_sharpe = detail_group.get("sharpe")
            if group.meta_weight is None and detail_group.get("meta_weight") is not None:
                group.meta_weight = detail_group.get("meta_weight")
            if not group.model_id or group.model_id == "-":
                group.model_id = detail_group.get("model_id") or group.model_id or "-"
            if group.factor_count == 0 and detail_group.get("factor_count") is not None:
                group.factor_count = int(detail_group.get("factor_count") or 0)
            if exp_status == "completed" and group.status in {"pending", "running"}:
                group.status = "completed"
            merged.append(group)
        return merged

    def _prioritize_recommendations(self, bottlenecks: list[Bottleneck]) -> list[Bottleneck]:
        """Sort bottlenecks by severity (high → medium → low)."""
        severity_order = {"high": 0, "medium": 1, "low": 2}
        return sorted(bottlenecks, key=lambda b: severity_order.get(b.severity, 3))

    def _ensure_analysis(
        self,
        analysis: dict[str, Any],
        groups: list[GroupMetrics],
        meta_info: dict[str, Any],
        correlations: dict[str, float],
    ) -> dict[str, Any]:
        """Return persisted analysis or derive a compact DB-only version."""
        if isinstance(analysis, dict) and analysis.get("schema_version"):
            return analysis

        group_diagnostics = []
        weighted_group_ic = 0.0
        best_group_ic = None
        for group in groups:
            contribution = None
            if group.group_ic is not None and group.meta_weight is not None:
                contribution = group.group_ic * group.meta_weight
                weighted_group_ic += contribution
            if group.group_ic is not None:
                best_group_ic = group.group_ic if best_group_ic is None else max(best_group_ic, group.group_ic)
            group_diagnostics.append({
                "group_name": group.group_name,
                "model_id": group.model_id,
                "factor_count": group.factor_count,
                "meta_weight": group.meta_weight,
                "group_ic": group.group_ic,
                "group_icir": group.group_icir,
                "group_sharpe": group.group_sharpe,
                "contribution_to_combined_ic": contribution,
                "data_available": {
                    "enhanced_metrics": False,
                    "ic_diagnostics": False,
                    "training_diagnostics": False,
                    "prediction_diagnostics": False,
                    "feature_importance": False,
                },
                "ic_diagnostics": {},
                "training_diagnostics": {},
                "prediction_diagnostics": {},
                "feature_importance_top": [],
            })

        weights = [g.meta_weight for g in groups if g.meta_weight is not None]
        hhi = sum(w * w for w in weights) if weights else None
        effective_groups = (1.0 / hhi) if hhi and hhi > 0 else None
        dominant = None
        if weights:
            dominant_group = max(groups, key=lambda g: g.meta_weight or 0)
            dominant = {
                "name": dominant_group.group_name,
                "weight": dominant_group.meta_weight,
            }

        corr_values = [abs(v) for v in correlations.values() if isinstance(v, (int, float))]
        high_corr_pairs = [
            {"pair": k, "correlation": v}
            for k, v in correlations.items()
            if isinstance(v, (int, float)) and abs(v) >= 0.7
        ]

        derived = {
            "schema_version": 1,
            "combined_vs_groups": {
                "combined_ic": meta_info.get("combined_ic"),
                "best_group_ic": best_group_ic,
                "weighted_group_ic": weighted_group_ic if group_diagnostics else None,
            },
            "portfolio_diagnostics": {},
            "diversification": {
                "weight_sum": sum(weights) if weights else None,
                "weight_hhi": hhi,
                "effective_group_count": effective_groups,
                "dominant_group": dominant.get("name") if dominant else None,
                "dominant_weight": dominant.get("weight") if dominant else None,
                "avg_abs_correlation": (sum(corr_values) / len(corr_values)) if corr_values else None,
                "max_abs_correlation": max(corr_values) if corr_values else None,
                "high_correlation_pairs": high_corr_pairs,
            },
            "group_diagnostics": group_diagnostics,
            "data_availability": {
                "combined_enhanced_metrics": False,
                "combined_training_diagnostics": False,
                "ic_quality": False,
                "groups_with_enhanced_metrics": 0,
                "groups_total": len(groups),
                "missing_group_enhanced_metrics": [g.group_name for g in groups],
            },
            "ic_quality": {},
        }
        try:
            from .multi_alpha_result_collector import MultiAlphaResultCollector

            derived["optimization_guidance"] = (
                MultiAlphaResultCollector()._generate_multi_alpha_guidance(derived)
            )
        except Exception as e:
            logger.error(f"Failed to derive multi-alpha optimization guidance: {e}")
            derived["optimization_guidance"] = []
        return derived

    @staticmethod
    def _parse_jsonish(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return None
        return None

    def _parse_factor_names(self, value: Any) -> list[str]:
        parsed = self._parse_jsonish(value)
        if isinstance(parsed, list):
            return parsed
        if isinstance(value, list):
            return value
        return []
