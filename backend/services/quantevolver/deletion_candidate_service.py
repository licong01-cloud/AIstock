"""Factor deletion candidate analysis service.

Three-tier identification (after applying 5 immunity rules):
  1. Exact twins    (|corr| >= 0.999)   — mathematical duplicates
  2. Pure noise     (ALL conditions)    — zero value across horizons/time/score
  3. Fuzzy twins    (0.98 <= |corr| < 0.999) — redundant but not identical

Immunity (ANY rule → keep):
  I1. max(|rank_ic_{1,5,10,20d}|) >= 0.02             — any-horizon predictive
  I2. monthly_ic_trend_slope > 0 AND sign_cons >= 0.6 — late bloomer
  I3. ic_oos_is_ratio >= 0.6 AND |rank_ic_mean| >= 0.015 — OOS stable
  I4. top_excess_sharpe >= 0.8                        — strong long-only
  I5. source='manual' AND coverage >= 0.5             — human-designed

Deletion does NOT happen here — this service only returns candidates.
UI / user confirmation + existing /quantevolver/factors/batch executes.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from ...db.pg_pool import get_conn
from .factor_official_evaluation_service import CALC_ENGINE

logger = logging.getLogger(__name__)


class DeletionCandidateService:

    DEFAULT_THRESHOLDS: Dict[str, float] = {
        "exact_twin_corr": 0.999,
        "fuzzy_twin_corr": 0.98,
        # Immunity
        "immune_max_abs_rank_ic": 0.02,
        "immune_trend_slope_positive": 0.0,
        "immune_sign_consistency_12m": 0.6,
        "immune_oos_is_ratio": 0.6,
        "immune_oos_abs_rank_ic_mean": 0.015,
        "immune_sharpe": 0.8,
        "immune_manual_coverage": 0.5,
        # Pure noise
        "noise_max_abs_rank_ic": 0.015,
        "noise_abs_rank_ic_mean": 0.01,
        "noise_abs_rank_icir_ann": 0.3,
        "noise_top_excess_sharpe": 0.3,
        "noise_sign_consistency_12m": 0.55,
        # (移除) noise_v2_score — v2 分数是 RDAgent 产出, 不代表真实价值, 不再作为噪声判定的保护条件
    }

    # ────────────────────────────────────────────────────────────────
    # Public entry point
    # ────────────────────────────────────────────────────────────────
    def analyze(self, thresholds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        t = {**self.DEFAULT_THRESHOLDS, **(thresholds or {})}

        factors = self._load_factors()
        corr_pairs = self._load_high_corr_pairs(min_corr=float(t["fuzzy_twin_corr"]))

        by_id: Dict[int, Dict[str, Any]] = {f["factor_catalog_id"]: f for f in factors}

        immune_ids = self._apply_immunity(factors, t)

        # Exact twins
        exact_groups = self._build_twin_groups(corr_pairs, by_id, min_corr=float(t["exact_twin_corr"]))
        deleted_ids: Set[int] = set()
        exact_candidates = self._pick_losers(exact_groups, immune_ids, corr_pairs, reason="exact_twin")
        deleted_ids.update(c["factor_catalog_id"] for c in exact_candidates)

        # Pure noise (scan remaining)
        noise_candidates: List[Dict[str, Any]] = []
        for f in factors:
            fid = f["factor_catalog_id"]
            if fid in immune_ids or fid in deleted_ids:
                continue
            if self._is_pure_noise(f, t):
                noise_candidates.append({**f, "deletion_reason": "pure_noise"})
        deleted_ids.update(c["factor_catalog_id"] for c in noise_candidates)

        # Fuzzy twins — exclude already-deleted ids before grouping
        remaining_pairs = [
            p for p in corr_pairs
            if p["a_id"] not in deleted_ids and p["b_id"] not in deleted_ids
        ]
        fuzzy_groups = self._build_twin_groups(
            remaining_pairs, by_id,
            min_corr=float(t["fuzzy_twin_corr"]),
            max_corr=float(t["exact_twin_corr"]),
        )
        fuzzy_candidates = self._pick_losers(fuzzy_groups, immune_ids, corr_pairs, reason="fuzzy_twin")
        deleted_ids.update(c["factor_catalog_id"] for c in fuzzy_candidates)

        return {
            "thresholds": t,
            "total_factors": len(factors),
            "immune_count": len(immune_ids),
            "exact_twins": exact_candidates,
            "pure_noise": noise_candidates,
            "fuzzy_twins": fuzzy_candidates,
            "total_candidates": len(exact_candidates) + len(noise_candidates) + len(fuzzy_candidates),
            "remaining_keep": len(factors) - len(deleted_ids),
        }

    # ────────────────────────────────────────────────────────────────
    # Data loading
    # ────────────────────────────────────────────────────────────────
    def _load_factors(self) -> List[Dict[str, Any]]:
        """Join catalog + authoritative metrics + latest monthly_ic + active official rating."""
        from .factor_rating_service import factor_rating_service

        rules = factor_rating_service.list_rule_versions()
        active_rule_version = rules.get("active_version") or rules.get("default_version")
        sql = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (factor_catalog_id) *
            FROM aistock_factor_metrics
            WHERE factor_catalog_id IS NOT NULL
              AND eval_window = 'full'
              AND calc_engine = %s
            ORDER BY factor_catalog_id, snapshot_date DESC NULLS LAST, created_at DESC NULLS LAST
        ),
        latest_monthly AS (
            SELECT DISTINCT ON (factor_name)
                factor_name,
                sign_consistency_12m AS ic_sign_consistency_12m,
                trend_slope_12m       AS monthly_ic_trend_slope,
                oos_is_ratio          AS ic_oos_is_ratio
            FROM aistock_factor_monthly_ic
            ORDER BY factor_name, month_end DESC
        ),
        latest_rating AS (
            SELECT DISTINCT ON (factor_catalog_id)
                factor_catalog_id,
                official_grade,
                official_score
            FROM qe_factor_official_ratings
            WHERE rule_version = %s
            ORDER BY factor_catalog_id, snapshot_date DESC NULLS LAST
        )
        SELECT
            c.id            AS factor_catalog_id,
            c.factor_name,
            c.source,
            c.is_available,
            c.disable_reason,
            m.ic_mean,
            m.rank_ic_mean,
            m.icir,
            m.rank_icir,
            m.rank_icir_annualized,
            m.rank_ic_1d,
            m.rank_ic_5d,
            m.rank_ic_10d,
            m.rank_ic_20d,
            m.ic_positive_ratio,
            m.top_excess_sharpe,
            m.top_excess_annual_return,
            m.group_return_monotonicity,
            m.turnover,
            m.coverage,
            m.direction,
            lm.ic_sign_consistency_12m,
            lm.monthly_ic_trend_slope,
            lm.ic_oos_is_ratio,
            lr.official_grade,
            lr.official_score,
            lr.official_grade AS v2_grade,
            lr.official_score AS v2_score
        FROM aistock_factor_catalog c
        LEFT JOIN latest_metrics           m  ON m.factor_catalog_id = c.id
        LEFT JOIN latest_monthly           lm ON lm.factor_name      = c.factor_name
        LEFT JOIN latest_rating            lr ON lr.factor_catalog_id = c.id
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (CALC_ENGINE, active_rule_version))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _load_high_corr_pairs(self, min_corr: float) -> List[Dict[str, Any]]:
        sql = """
        SELECT factor_a_id AS a_id, factor_b_id AS b_id, correlation, method
        FROM qe_factor_correlations
        WHERE ABS(correlation) >= %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (min_corr,))
                return [
                    {"a_id": r[0], "b_id": r[1], "corr": float(r[2]), "method": r[3]}
                    for r in cur.fetchall()
                ]

    # ────────────────────────────────────────────────────────────────
    # Immunity
    # ────────────────────────────────────────────────────────────────
    def _apply_immunity(self, factors: List[Dict[str, Any]], t: Dict[str, float]) -> Set[int]:
        immune: Set[int] = set()
        for f in factors:
            fid = f["factor_catalog_id"]
            horizons = [f.get("rank_ic_1d"), f.get("rank_ic_5d"),
                        f.get("rank_ic_10d"), f.get("rank_ic_20d")]
            max_abs_h = max((abs(h) for h in horizons if h is not None), default=0.0)

            # I1: any horizon predictive
            if max_abs_h >= t["immune_max_abs_rank_ic"]:
                f["immune_reason"] = "any_horizon_strong"
                immune.add(fid); continue

            # I2: late bloomer
            slope = f.get("monthly_ic_trend_slope")
            sign_cons = f.get("ic_sign_consistency_12m")
            if (slope is not None and slope > t["immune_trend_slope_positive"]
                and sign_cons is not None and sign_cons >= t["immune_sign_consistency_12m"]):
                f["immune_reason"] = "late_bloomer"
                immune.add(fid); continue

            # I3: OOS stable
            oos = f.get("ic_oos_is_ratio")
            ric_mean = f.get("rank_ic_mean")
            if (oos is not None and oos >= t["immune_oos_is_ratio"]
                and ric_mean is not None and abs(ric_mean) >= t["immune_oos_abs_rank_ic_mean"]):
                f["immune_reason"] = "oos_stable"
                immune.add(fid); continue

            # I4: high sharpe
            sharpe = f.get("top_excess_sharpe")
            if sharpe is not None and sharpe >= t["immune_sharpe"]:
                f["immune_reason"] = "high_sharpe"
                immune.add(fid); continue

            # I5: manual with coverage
            if f.get("source") == "manual":
                cov = f.get("coverage")
                if cov is not None and cov >= t["immune_manual_coverage"]:
                    f["immune_reason"] = "manual_with_coverage"
                    immune.add(fid); continue

        return immune

    # ────────────────────────────────────────────────────────────────
    # Twin groups (Union-Find)
    # ────────────────────────────────────────────────────────────────
    def _build_twin_groups(
        self,
        pairs: List[Dict[str, Any]],
        by_id: Dict[int, Dict[str, Any]],
        min_corr: float,
        max_corr: float = 2.0,
    ) -> List[List[Dict[str, Any]]]:
        parent: Dict[int, int] = {}

        def find(x: int) -> int:
            while parent.get(x, x) != x:
                parent[x] = parent.get(parent[x], parent[x])
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for p in pairs:
            c = abs(p["corr"])
            if c < min_corr or c >= max_corr:
                continue
            parent.setdefault(p["a_id"], p["a_id"])
            parent.setdefault(p["b_id"], p["b_id"])
            union(p["a_id"], p["b_id"])

        # Collect groups
        groups_map: Dict[int, List[int]] = {}
        for node in list(parent.keys()):
            root = find(node)
            groups_map.setdefault(root, []).append(node)

        # Materialise as factor dicts
        result: List[List[Dict[str, Any]]] = []
        for members in groups_map.values():
            if len(members) < 2:
                continue
            group = [by_id[m] for m in members if m in by_id]
            if len(group) >= 2:
                result.append(group)
        return result

    def _pick_losers(
        self,
        groups: List[List[Dict[str, Any]]],
        immune_ids: Set[int],
        corr_pairs: List[Dict[str, Any]],
        reason: str,
    ) -> List[Dict[str, Any]]:
        """Within each group, pick best by composite score; others become deletion candidates.

        Immune factors never become losers. If the group contains immune factors,
        the non-immune members are all candidates (immune ones are already kept).
        """
        corr_lookup: Dict[Tuple[int, int], float] = {}
        for p in corr_pairs:
            key = (min(p["a_id"], p["b_id"]), max(p["a_id"], p["b_id"]))
            corr_lookup[key] = max(abs(p["corr"]), corr_lookup.get(key, 0.0))

        losers: List[Dict[str, Any]] = []
        for group in groups:
            non_immune = [f for f in group if f["factor_catalog_id"] not in immune_ids]
            if not non_immune:
                continue
            # If ANY member is immune, all non-immune are losers (immune is "best")
            if len(non_immune) < len(group):
                immune_peer = next(f for f in group if f["factor_catalog_id"] in immune_ids)
                kept_name = immune_peer["factor_name"]
                for loser in non_immune:
                    key = (min(loser["factor_catalog_id"], immune_peer["factor_catalog_id"]),
                           max(loser["factor_catalog_id"], immune_peer["factor_catalog_id"]))
                    losers.append({
                        **loser,
                        "deletion_reason": reason,
                        "twin_kept": kept_name,
                        "twin_kept_id": immune_peer["factor_catalog_id"],
                        "twin_corr": corr_lookup.get(key, 0.0),
                        "kept_is_immune": True,
                    })
                continue
            # Else rank within non_immune
            sorted_group = sorted(non_immune, key=self._composite_score, reverse=True)
            best = sorted_group[0]
            for loser in sorted_group[1:]:
                key = (min(loser["factor_catalog_id"], best["factor_catalog_id"]),
                       max(loser["factor_catalog_id"], best["factor_catalog_id"]))
                losers.append({
                    **loser,
                    "deletion_reason": reason,
                    "twin_kept": best["factor_name"],
                    "twin_kept_id": best["factor_catalog_id"],
                    "twin_corr": corr_lookup.get(key, 0.0),
                    "kept_is_immune": False,
                })
        return losers

    def _composite_score(self, f: Dict[str, Any]) -> Tuple[float, float, float, int]:
        """Higher tuple → better. Tiebreak by lower id (earlier-inserted preferred).

        SOTA/v2_score 已移除: 不再作为 twin 保留的排序依据, 避免因子库清洗对 RDAgent 产物的偏袒.
        优先级: coverage > |rank_icir_annualized| > earlier id.
        """
        return (
            float(f.get("coverage") or 0),
            abs(float(f.get("rank_icir_annualized") or 0)),
            abs(float(f.get("rank_ic_mean") or 0)),
            -int(f.get("factor_catalog_id") or 0),
        )

    # ────────────────────────────────────────────────────────────────
    # Pure noise
    # ────────────────────────────────────────────────────────────────
    def _is_pure_noise(self, f: Dict[str, Any], t: Dict[str, float]) -> bool:
        horizons = [f.get("rank_ic_1d"), f.get("rank_ic_5d"),
                    f.get("rank_ic_10d"), f.get("rank_ic_20d")]
        max_abs_h = max((abs(h) for h in horizons if h is not None), default=0.0)

        ric_mean = abs(f.get("rank_ic_mean") or 0)
        ricir = abs(f.get("rank_icir_annualized") or 0)
        tsharpe = float(f.get("top_excess_sharpe") or 0)
        slope = f.get("monthly_ic_trend_slope")
        sign_cons = f.get("ic_sign_consistency_12m")

        # SOTA/v2_score 已移除: 高 v2_score 不再豁免噪声判定 (v2 是 RDAgent 打分, 不是真实信号强度)
        return (
            max_abs_h < t["noise_max_abs_rank_ic"]
            and ric_mean < t["noise_abs_rank_ic_mean"]
            and ricir < t["noise_abs_rank_icir_ann"]
            and tsharpe < t["noise_top_excess_sharpe"]
            and (slope is None or slope <= 0)
            and (sign_cons is None or sign_cons < t["noise_sign_consistency_12m"])
        )


deletion_candidate_service = DeletionCandidateService()
