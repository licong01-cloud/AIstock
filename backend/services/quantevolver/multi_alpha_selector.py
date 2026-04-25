"""
Multi-Alpha Factor Selection Engine (Phase 3).

科学三阶段筛选架构：
  Stage 1: 基础质量筛选 (grade + best_horizon_IC)
  Stage 2: 数据源分组 + 组内相关性去冗余
  Stage 3: 组内稀疏精选 (Conditional IC 排序)

融合 AlphaForge (AAAI 2025) + Elastic Net + CPCV 设计理念。
实际 Elastic Net / CPCV 在 v2 实现；v1 用 Conditional IC + 相关性过滤。
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from ...db.pg_pool import get_conn
from .experiment_config import AlphaGroup, MetaModelConfig, MultiAlphaConfig
from .factor_official_evaluation_service import CALC_ENGINE

logger = logging.getLogger("aistock.quantevolver.multi_alpha_selector")


# ── 默认组模板：data_source_group → (推荐 model_id, dataset_type, compute) ──

_GROUP_TEMPLATES: dict[str, dict[str, Any]] = {
    "price_volume": {
        "model_id": "__seed_ALSTM_default_v1__",
        "dataset_type": "TSDatasetH",
        "compute_resource": "gpu",
        "holding_period_hint": "medium",
    },
    "money_flow": {
        "model_id": "__seed_LGBModel_conservative_v1__",
        "dataset_type": "DatasetH",
        "compute_resource": "cpu",
        "holding_period_hint": "medium",
    },
    "fundamental": {
        "model_id": "__seed_Ridge_default_v1__",
        "dataset_type": "DatasetH",
        "compute_resource": "cpu",
        "holding_period_hint": "long",
    },
    "valuation": {
        "model_id": "__seed_LGBModel_conservative_v1__",
        "dataset_type": "DatasetH",
        "compute_resource": "cpu",
        "holding_period_hint": "medium",
    },
    "chip": {
        "model_id": "__seed_LGBModel_conservative_v1__",
        "dataset_type": "DatasetH",
        "compute_resource": "cpu",
        "holding_period_hint": "medium",
    },
    "sector": {
        "model_id": "__seed_ALSTM_sector_v1__",
        "dataset_type": "TSDatasetH",
        "compute_resource": "gpu",
        "holding_period_hint": "medium",
    },
    "liquidity": {
        "model_id": "__seed_LGBModel_conservative_v1__",
        "dataset_type": "DatasetH",
        "compute_resource": "cpu",
        "holding_period_hint": "medium",
    },
    "cross_dataset": {
        "model_id": "__seed_LGBModel_conservative_v1__",
        "dataset_type": "DatasetH",
        "compute_resource": "cpu",
        "holding_period_hint": "medium",
    },
}

# 组合并规则：因子数 < MIN_GROUP_SIZE 的组并入最近的同 dataset_type 组
_MIN_GROUP_SIZE = 5


class MultiAlphaFactorSelector:
    """多 Alpha 因子选择引擎。

    Usage:
        selector = MultiAlphaFactorSelector()
        config = selector.auto_select()
        # config is a MultiAlphaConfig ready for ExperimentConfig
    """

    def auto_select(
        self,
        min_grade: str = "C",
        min_ic: float = 0.02,
        max_factors_per_group: int = 15,
        max_intra_corr: float = 0.7,
        holding_period_filter: Optional[list[str]] = None,
        meta_method: str = "ic_weighted",
        execution_mode: str = "serial",
    ) -> MultiAlphaConfig:
        """自动因子分组选择 — 三阶段筛选。

        Args:
            min_grade: 最低评级 ("S"/"A"/"B"/"C")
            min_ic: best_horizon_IC 最低阈值
            max_factors_per_group: 每组最多因子数
            max_intra_corr: 组内最大相关系数
            holding_period_filter: 只保留特定持有期 (e.g. ["medium", "long"])
            meta_method: Meta-Model 方法 ("ic_weighted"/"ols"/"stacking")
            execution_mode: 执行模式 ("serial"/"local_parallel"/"distributed")

        Returns:
            MultiAlphaConfig with auto-populated alpha_groups
        """
        # ── Stage 1: Quality filter ──────────────────────────────────
        candidates = self._stage1_quality_filter(min_grade, min_ic, holding_period_filter)
        logger.info(f"Stage 1: {len(candidates)} factors passed quality filter")

        if len(candidates) < 4:
            raise ValueError(
                f"Only {len(candidates)} factors passed quality filter "
                f"(need ≥4 for multi-alpha). Try lowering min_grade or min_ic."
            )

        # ── Stage 2: Group + dedup ──────────────────────────────────
        groups = self._stage2_group_and_dedup(candidates, max_intra_corr)
        logger.info(f"Stage 2: {len(groups)} groups formed")

        # ── Stage 3: Intra-group sparse selection ───────────────────
        groups = self._stage3_select(groups, max_factors_per_group)

        # 合并小组
        groups = self._merge_small_groups(groups)
        logger.info(f"After merge: {len(groups)} groups, total {sum(len(g['factors']) for g in groups)} factors")

        if len(groups) < 2:
            raise ValueError(
                f"Only {len(groups)} group(s) after merge — need ≥2. "
                f"Try lowering min_ic to include more factors."
            )

        # ── Build MultiAlphaConfig ──────────────────────────────────
        alpha_groups = []
        for g in groups:
            tmpl = _GROUP_TEMPLATES.get(g["group_name"], _GROUP_TEMPLATES["cross_dataset"])
            alpha_groups.append(AlphaGroup(
                group_name=g["group_name"],
                factor_names=[f["factor_name"] for f in g["factors"]],
                model_id=tmpl["model_id"],
                dataset_type=tmpl["dataset_type"],
                compute_resource=tmpl["compute_resource"],
                holding_period_hint=tmpl.get("holding_period_hint"),
            ))

        return MultiAlphaConfig(
            alpha_groups=alpha_groups,
            meta_model=MetaModelConfig(method=meta_method),
            execution_mode=execution_mode,
            auto_selected=True,
        )

    def get_group_templates(self) -> dict[str, dict[str, Any]]:
        """返回预定义分组模板（前端 UI 展示用）。"""
        return dict(_GROUP_TEMPLATES)

    def preview(self, config: MultiAlphaConfig) -> dict[str, Any]:
        """预览 Multi-Alpha 配置统计。"""
        groups_info = []
        total_factors = 0
        for g in config.alpha_groups:
            groups_info.append({
                "group_name": g.group_name,
                "factor_count": len(g.factor_names),
                "model_id": g.model_id,
                "dataset_type": g.dataset_type,
                "compute_resource": g.compute_resource,
            })
            total_factors += len(g.factor_names)
        return {
            "total_groups": len(config.alpha_groups),
            "total_factors": total_factors,
            "meta_method": config.meta_model.method,
            "execution_mode": config.execution_mode,
            "groups": groups_info,
        }

    # ── Internal stages ──────────────────────────────────────────────

    def _stage1_quality_filter(
        self,
        min_grade: str,
        min_ic: float,
        holding_period_filter: Optional[list[str]],
    ) -> list[dict]:
        """Stage 1: 基础质量筛选。

        Uses best_horizon_IC = max(|rank_ic_1d|, |rank_ic_5d|, |rank_ic_10d|, |rank_ic_20d|)
        so long-horizon factors are not penalized by low 1d IC.
        """
        grade_order = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4, "P": 5}
        min_val = grade_order.get(min_grade, 3)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT fc.factor_name, fc.factor_source, fc.category,
                           fr.official_grade AS grade,
                           fc.ic_value, fc.data_source_group, fc.holding_period_class,
                           fc.ts_info_density, fc.linearity,
                           fm.rank_ic_1d, fm.rank_ic_5d, fm.rank_ic_10d, fm.rank_ic_20d,
                           fm.icir_annualized, fm.rank_icir_annualized
                    FROM qe_factor_classification fc
                    JOIN aistock_factor_catalog cat
                        ON cat.factor_name = fc.factor_name AND cat.source = fc.factor_source
                    LEFT JOIN LATERAL (
                        SELECT official_grade FROM qe_factor_official_ratings r
                        WHERE r.factor_catalog_id = cat.id
                          AND r.rule_version = (
                              SELECT rule_version FROM qe_rating_rule_versions
                              WHERE status = 'active' ORDER BY activated_at DESC LIMIT 1
                          )
                        ORDER BY r.graded_at DESC LIMIT 1
                    ) fr ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
                               icir_annualized, rank_icir_annualized
                        FROM aistock_factor_metrics
                        WHERE factor_name = fc.factor_name AND eval_window = 'full' AND calc_engine = %s
                        ORDER BY calculated_at DESC LIMIT 1
                    ) fm ON TRUE
                    WHERE cat.is_available = TRUE
                      AND fc.data_source_group IS NOT NULL
                      AND fc.data_source_group != 'unknown'
                    ORDER BY fr.official_grade, ABS(fc.ic_value) DESC NULLS LAST
                """, (CALC_ENGINE,))
                cols = [d[0] for d in cur.description]
                all_factors = [dict(zip(cols, r)) for r in cur.fetchall()]

        candidates = []
        for f in all_factors:
            # Grade filter
            if grade_order.get(f["grade"], 5) > min_val:
                continue

            # Holding period filter
            if holding_period_filter and f.get("holding_period_class") not in holding_period_filter:
                continue

            # best_horizon_IC = max across all periods
            ics = [
                abs(f.get("rank_ic_1d") or 0),
                abs(f.get("rank_ic_5d") or 0),
                abs(f.get("rank_ic_10d") or 0),
                abs(f.get("rank_ic_20d") or 0),
                abs(f.get("ic_value") or 0),
            ]
            f["best_horizon_ic"] = max(ics)

            if f["best_horizon_ic"] < min_ic:
                continue

            candidates.append(f)

        return candidates

    def _stage2_group_and_dedup(
        self,
        candidates: list[dict],
        max_intra_corr: float,
    ) -> list[dict]:
        """Stage 2: 按 data_source_group 分组 + 组内相关性去冗余。"""

        # 加载相关性数据
        corr_map = self._load_correlations(
            [f["factor_name"] for f in candidates]
        )

        # 按 data_source_group 分组
        group_map: dict[str, list[dict]] = {}
        for f in candidates:
            ds = f.get("data_source_group") or "cross_dataset"
            group_map.setdefault(ds, []).append(f)

        result = []
        for group_name, factors in group_map.items():
            # 排序: best_horizon_ic 降序
            factors.sort(key=lambda f: f["best_horizon_ic"], reverse=True)

            # Greedy 去冗余: 按 IC 从高到低选，跳过与已选因子相关性过高的
            selected = []
            for f in factors:
                too_similar = False
                for sel in selected:
                    pair_key = _corr_key(f["factor_name"], sel["factor_name"])
                    corr = abs(corr_map.get(pair_key, 0.0))
                    if corr > max_intra_corr:
                        too_similar = True
                        break
                if not too_similar:
                    selected.append(f)

            if selected:
                result.append({
                    "group_name": group_name,
                    "factors": selected,
                })

        return result

    def _stage3_select(
        self,
        groups: list[dict],
        max_per_group: int,
    ) -> list[dict]:
        """Stage 3: 组内精选（截断到 max_per_group）。

        v1: 简单截断（已按 best_horizon_ic 排序）。
        v2: 可改为 Elastic Net + Conditional IC。
        """
        for g in groups:
            g["factors"] = g["factors"][:max_per_group]
        return groups

    def _merge_small_groups(self, groups: list[dict]) -> list[dict]:
        """合并小组（因子数 < MIN_GROUP_SIZE）到最近的同 dataset_type 组。"""
        small = [g for g in groups if len(g["factors"]) < _MIN_GROUP_SIZE]
        big = [g for g in groups if len(g["factors"]) >= _MIN_GROUP_SIZE]

        if not small:
            return groups

        for sg in small:
            sg_ds = _GROUP_TEMPLATES.get(sg["group_name"], {}).get("dataset_type", "DatasetH")
            # 找同 dataset_type 的最大组
            best_target = None
            best_size = 0
            for bg in big:
                bg_ds = _GROUP_TEMPLATES.get(bg["group_name"], {}).get("dataset_type", "DatasetH")
                if bg_ds == sg_ds and len(bg["factors"]) > best_size:
                    best_target = bg
                    best_size = len(bg["factors"])

            if best_target:
                # 合并（避免重复因子）
                existing_names = {f["factor_name"] for f in best_target["factors"]}
                for f in sg["factors"]:
                    if f["factor_name"] not in existing_names:
                        best_target["factors"].append(f)
                        existing_names.add(f["factor_name"])
                logger.info(
                    f"Merged group '{sg['group_name']}' ({len(sg['factors'])} factors) "
                    f"into '{best_target['group_name']}'"
                )
            else:
                # 无合适目标，保留小组
                big.append(sg)

        return big

    def _load_correlations(self, factor_names: list[str]) -> dict[str, float]:
        """从 qe_factor_correlations 加载因子对的相关系数。"""
        if len(factor_names) < 2:
            return {}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ca.factor_name, cb.factor_name, qc.correlation
                    FROM qe_factor_correlations qc
                    JOIN aistock_factor_catalog ca ON ca.id = qc.factor_a_id
                    JOIN aistock_factor_catalog cb ON cb.id = qc.factor_b_id
                    WHERE ca.factor_name = ANY(%s) AND cb.factor_name = ANY(%s)
                """, (factor_names, factor_names))
                result = {}
                for r in cur.fetchall():
                    key = _corr_key(r[0], r[1])
                    result[key] = r[2]
                return result


def _corr_key(a: str, b: str) -> str:
    """Canonical key for a factor pair (sorted to ensure a < b)."""
    return f"{min(a,b)}|{max(a,b)}"
