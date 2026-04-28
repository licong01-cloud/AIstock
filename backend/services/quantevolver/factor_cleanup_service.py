"""Factor library cleanup service (productized from cleanup scripts).

实现三条清理规则的 dry-run 预览 + 实际执行:

  Rule A   — near_identical:  qe_factor_classification.cluster_role = 'member'
                              (complete-linkage + raw corr + 阈值 0.999 簇内非 rep)
  Rule B v2 — pure_noise_v2:  grade='D' AND |ic_mean|<0.003 AND |rank_ic_mean|<0.003
                              AND ic_positive_ratio∈[0.45,0.55] AND |rank_icir|<0.1
  Rule A'  — reverse_redundant: corr ≤ -0.999 启用对去重
                              corr=-1 精确: 留 ic_mean ≥ 0; 否则留 |ic_mean| 大

工作流:
  preview(rules)        → 返回候选清单 (与 /factors 列结构一致), 不写库
  execute(items, batch) → 按 disable_reason 分组写库

UI 流程:
  1. 用户点 "因子清洗" 按钮 → POST /factors/cleanup/preview
  2. 弹窗展示候选 (与因子库相同的列)
  3. 用户勾选确认 → POST /factors/cleanup/execute
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from ...db.pg_pool import get_conn
from .factor_official_evaluation_service import CALC_ENGINE

logger = logging.getLogger("aistock.quantevolver.factor_cleanup_service")


class FactorCleanupService:

    DEFAULTS: Dict[str, float] = {
        "ic_th": 0.003,
        "rank_ic_th": 0.003,
        "pos_ratio_lo": 0.45,
        "pos_ratio_hi": 0.55,
        "rank_icir_th": 0.1,
        "neg_corr_th": -0.999,
        "exact_neg": -0.9999,
    }

    REASON_NEAR_IDENTICAL = "v2_cleanup:near_identical"
    REASON_PURE_NOISE = "v2_cleanup:pure_noise_v2"
    REASON_REVERSE_REDUNDANT = "v2_cleanup:reverse_redundant"

    SUPPORTED_RULES = ("near_identical", "pure_noise_v2", "reverse_redundant")

    # ────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────
    def preview(
        self,
        rules: Optional[List[str]] = None,
        thresholds: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """干跑预览, 返回所有规则命中的候选 (字段与 /factors 列表一致).

        返回:
            {
                "summary": {
                    "total_enabled": int,
                    "total_candidates": int,
                    "by_rule": {"near_identical": N, "pure_noise_v2": N, "reverse_redundant": N},
                    "thresholds": {...},
                },
                "candidates": [{factor_name, source, ic, ind_ic, ind_rank_ic, ..., cleanup_reason, cleanup_rule, cleanup_detail}, ...],
                "reverse_pairs": [{keep, drop, corr, reason}, ...],  # Rule A' 配对详情
            }
        """
        active_rules = set(rules or self.SUPPORTED_RULES)
        unknown = active_rules - set(self.SUPPORTED_RULES)
        if unknown:
            raise ValueError(f"未知规则: {unknown}; 支持: {self.SUPPORTED_RULES}")

        t = {**self.DEFAULTS, **(thresholds or {})}

        with get_conn() as conn:
            factors_full = self._load_factors_full(conn)
            total_enabled = len(factors_full)

            # 索引: id → row
            by_id: Dict[int, Dict[str, Any]] = {f["id"]: f for f in factors_full}

            hits: Dict[int, Dict[str, Any]] = {}  # id → {rule, reason, detail}
            reverse_pairs: List[Dict[str, Any]] = []

            # Rule A — near_identical
            if "near_identical" in active_rules:
                for fid, f in by_id.items():
                    if f.get("cluster_role") == "member":
                        hits.setdefault(fid, {
                            "rule": "near_identical",
                            "reason": self.REASON_NEAR_IDENTICAL,
                            "detail": f"cluster_id={f.get('cluster_id')} (complete-linkage 0.999)",
                        })

            # Rule B v2 — pure_noise_v2
            if "pure_noise_v2" in active_rules:
                for fid, f in by_id.items():
                    if self._match_pure_noise(f, t):
                        # 优先级: A' > A > B
                        if fid not in hits:
                            hits[fid] = {
                                "rule": "pure_noise_v2",
                                "reason": self.REASON_PURE_NOISE,
                                "detail": (
                                    f"|ic|={abs(f['ind_ic'] or 0):.4f}, "
                                    f"|rank_ic|={abs(f['ind_rank_ic'] or 0):.4f}, "
                                    f"pos={(f['ic_positive_ratio'] or 0)*100:.1f}%"
                                ),
                            }

            # Rule A' — reverse_redundant
            if "reverse_redundant" in active_rules:
                neg_pairs = self._load_neg_corr_pairs(conn, t["neg_corr_th"], by_id)
                rule_a_prime = self._resolve_reverse_pairs(neg_pairs, by_id, t)
                for keep_id, drop_id, corr, pair_reason in rule_a_prime:
                    detail = (
                        f"corr={corr:+.4f}, 保留 {by_id[keep_id]['factor_name']} "
                        f"(ic={by_id[keep_id]['ind_ic'] or 0:+.4f})"
                    )
                    # A' 优先级最高 — 覆盖前面的判断
                    hits[drop_id] = {
                        "rule": "reverse_redundant",
                        "reason": self.REASON_REVERSE_REDUNDANT,
                        "detail": detail,
                    }
                    reverse_pairs.append({
                        "keep_id": keep_id,
                        "keep_name": by_id[keep_id]["factor_name"],
                        "keep_ic": by_id[keep_id]["ind_ic"],
                        "drop_id": drop_id,
                        "drop_name": by_id[drop_id]["factor_name"],
                        "drop_ic": by_id[drop_id]["ind_ic"],
                        "corr": corr,
                        "reason": pair_reason,
                    })

        # 组装候选列表 (与 /factors 列结构一致 + 三个 cleanup_* 字段)
        candidates: List[Dict[str, Any]] = []
        for fid, hit in hits.items():
            row = dict(by_id[fid])
            row["cleanup_rule"] = hit["rule"]
            row["cleanup_reason"] = hit["reason"]
            row["cleanup_detail"] = hit["detail"]
            candidates.append(row)

        # 按 rule + factor_name 排序
        rule_order = {"reverse_redundant": 0, "near_identical": 1, "pure_noise_v2": 2}
        candidates.sort(key=lambda r: (rule_order.get(r["cleanup_rule"], 9), r["factor_name"]))

        by_rule_count: Dict[str, int] = {r: 0 for r in self.SUPPORTED_RULES}
        for c in candidates:
            by_rule_count[c["cleanup_rule"]] = by_rule_count.get(c["cleanup_rule"], 0) + 1

        return {
            "summary": {
                "total_enabled": total_enabled,
                "total_candidates": len(candidates),
                "after_cleanup": total_enabled - len(candidates),
                "by_rule": by_rule_count,
                "thresholds": t,
                "rules_applied": sorted(active_rules),
            },
            "candidates": candidates,
            "reverse_pairs": reverse_pairs,
        }

    def execute(
        self,
        factor_ids: List[int],
        reasons: Dict[int, str],
        batch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """正式执行禁用. 必须由 UI 在 preview 后调用, 由用户人工确认.

        Args:
            factor_ids: 要禁用的 factor_catalog id 列表
            reasons: {id: disable_reason}, reason 必须是 self.REASON_* 之一
            batch_id: 批次号; 为空自动生成 v2_cleanup_YYYYmmdd_HHMMSS

        Returns:
            {ok, batch_id, disabled_count, by_reason: {reason: n}, errors: [...]}
        """
        if not factor_ids:
            raise ValueError("factor_ids 不能为空")

        valid_reasons = {
            self.REASON_NEAR_IDENTICAL,
            self.REASON_PURE_NOISE,
            self.REASON_REVERSE_REDUNDANT,
        }
        bad = [r for r in reasons.values() if r not in valid_reasons]
        if bad:
            raise ValueError(f"reason 必须是 {valid_reasons} 之一, 收到: {set(bad)}")

        # 必须每个 id 都有 reason
        missing = [fid for fid in factor_ids if fid not in reasons]
        if missing:
            raise ValueError(f"以下 id 缺少 reason: {missing}")

        if batch_id is None:
            batch_id = f"v2_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 按 reason 分组
        by_reason: Dict[str, List[int]] = {}
        for fid in factor_ids:
            r = reasons[fid]
            by_reason.setdefault(r, []).append(fid)

        result_by_reason: Dict[str, int] = {}
        errors: List[str] = []

        with get_conn() as conn:
            with conn.cursor() as cur:
                for reason, ids in by_reason.items():
                    try:
                        cur.execute(
                            """
                            UPDATE aistock_factor_catalog
                            SET is_available = FALSE,
                                disable_reason = %s,
                                disable_batch_id = %s,
                                disable_at = NOW(),
                                updated_at = NOW()
                            WHERE id = ANY(%s) AND is_available = TRUE
                            """,
                            (reason, batch_id, ids),
                        )
                        result_by_reason[reason] = cur.rowcount
                        # 顺手清理 correlation 记录, 与 batch-action set_unavailable 行为一致
                        cur.execute(
                            "DELETE FROM qe_factor_correlations "
                            "WHERE factor_a_id = ANY(%s) OR factor_b_id = ANY(%s)",
                            (ids, ids),
                        )
                    except Exception as exc:
                        errors.append(f"reason={reason}: {exc}")
                        logger.exception("cleanup execute failed for reason=%s", reason)

                # 刷新 correlation_pair_count: 被清洗的因子对已 DELETE, 需要让
                # 幸存因子的 pair_count 反映真实剩余 pair 数, 否则 FactorList 徽章
                # 会持续显示陈旧计数直到下次相关性重算.
                try:
                    cur.execute(
                        """
                        UPDATE aistock_factor_catalog c SET
                            correlation_pair_count = COALESCE(sub.cnt, 0)
                        FROM (
                            SELECT factor_id, COUNT(*) AS cnt FROM (
                                SELECT factor_a_id AS factor_id FROM qe_factor_correlations
                                UNION ALL
                                SELECT factor_b_id AS factor_id FROM qe_factor_correlations
                            ) t GROUP BY factor_id
                        ) sub
                        WHERE c.id = sub.factor_id
                        """
                    )
                    # 已完全无 pair 的因子 (包括被禁用的本批) 归零
                    cur.execute(
                        """
                        UPDATE aistock_factor_catalog
                        SET correlation_pair_count = 0
                        WHERE correlation_pair_count > 0
                          AND id NOT IN (
                              SELECT factor_a_id FROM qe_factor_correlations
                              UNION
                              SELECT factor_b_id FROM qe_factor_correlations
                          )
                        """
                    )
                except Exception as exc:
                    errors.append(f"pair_count refresh: {exc}")
                    logger.exception("pair_count refresh failed")

            conn.commit()

        total_disabled = sum(result_by_reason.values())
        logger.info(
            "Factor cleanup executed: batch=%s, total=%d, by_reason=%s",
            batch_id, total_disabled, result_by_reason,
        )
        return {
            "ok": len(errors) == 0,
            "batch_id": batch_id,
            "disabled_count": total_disabled,
            "by_reason": result_by_reason,
            "errors": errors,
            "rollback_sql": (
                f"UPDATE aistock_factor_catalog "
                f"SET is_available=TRUE, disable_reason=NULL, "
                f"disable_batch_id=NULL, disable_at=NULL "
                f"WHERE disable_batch_id='{batch_id}';"
            ),
        }

    def list_recent_batches(self, limit: int = 20) -> List[Dict[str, Any]]:
        """最近的 disable 批次, 用于 UI 显示历史."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT disable_batch_id, disable_reason, COUNT(*) AS n,
                           MIN(disable_at) AS first_at, MAX(disable_at) AS last_at
                    FROM aistock_factor_catalog
                    WHERE disable_batch_id IS NOT NULL
                    GROUP BY disable_batch_id, disable_reason
                    ORDER BY MAX(disable_at) DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    def rollback_batch(self, batch_id: str) -> Dict[str, Any]:
        """回滚指定批次 (重新启用)."""
        if not batch_id:
            raise ValueError("batch_id 不能为空")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE aistock_factor_catalog
                    SET is_available = TRUE,
                        disable_reason = NULL,
                        disable_batch_id = NULL,
                        disable_at = NULL,
                        updated_at = NOW()
                    WHERE disable_batch_id = %s
                    """,
                    (batch_id,),
                )
                rehab = cur.rowcount
            conn.commit()
        logger.info("Rollback batch=%s, rehab=%d", batch_id, rehab)
        return {"ok": True, "batch_id": batch_id, "rehab_count": rehab}

    # ────────────────────────────────────────────────────────────────
    # Internal
    # ────────────────────────────────────────────────────────────────
    def _load_factors_full(self, conn) -> List[Dict[str, Any]]:
        """加载启用因子 + 评级 + 1d out_sample metrics + 分类字段 (与 /factors 列对齐)."""
        from .factor_rating_service import factor_rating_service

        rules = factor_rating_service.list_rule_versions()
        active_rule_version = rules.get("active_version") or rules.get("default_version")
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH rated AS (
                    SELECT DISTINCT ON (factor_catalog_id)
                        factor_catalog_id, official_grade, official_score
                    FROM qe_factor_official_ratings
                    WHERE rule_version = %s
                    ORDER BY factor_catalog_id, graded_at DESC
                ),
                metrics AS (
                    SELECT DISTINCT ON (factor_catalog_id)
                        factor_catalog_id, ic_mean, rank_ic_mean,
                        icir, rank_icir, ic_positive_ratio, coverage,
                        top_excess_sharpe, top_excess_annual_return
                    FROM aistock_factor_metrics
                    WHERE eval_window = 'out_sample'
                      AND return_horizon = '1d'
                      AND calc_engine = %s
                    ORDER BY factor_catalog_id, calculated_at DESC
                )
                SELECT
                    a.id, a.factor_name, a.source, a.expression,
                    m.ic_mean AS ic,
                    m.top_excess_sharpe AS sharpe,
                    m.top_excess_annual_return AS annualized_return,
                    a.is_sota_factor,
                    a.description_cn, a.is_available,
                    r.official_grade, r.official_score,
                    m.ic_mean         AS ind_ic,
                    m.rank_ic_mean    AS ind_rank_ic,
                    m.icir            AS ind_icir,
                    m.rank_icir       AS ind_rank_icir,
                    m.ic_positive_ratio,
                    m.coverage,
                    m.top_excess_sharpe   AS ind_sharpe,
                    m.top_excess_annual_return AS ind_annual_return,
                    cl.category, cl.factor_dimension,
                    cl.cluster_id, cl.cluster_role, cl.cluster_size,
                    cl.ts_info_density, cl.cross_horizon_consistency,
                    cl.ic_sign_consistency_12m
                FROM aistock_factor_catalog a
                LEFT JOIN rated r ON r.factor_catalog_id = a.id
                LEFT JOIN metrics m ON m.factor_catalog_id = a.id
                LEFT JOIN qe_factor_classification cl
                    ON cl.factor_name = a.factor_name AND cl.factor_source = a.source
                WHERE a.is_available = TRUE
                """,
                (active_rule_version, CALC_ENGINE),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _load_neg_corr_pairs(
        self, conn, threshold: float, by_id: Dict[int, Dict[str, Any]]
    ) -> List[Tuple[int, int, float]]:
        """加载 corr ≤ threshold 的启用因子对 (双向都启用)."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (factor_a_id, factor_b_id)
                    factor_a_id, factor_b_id, correlation
                FROM qe_factor_correlations
                ORDER BY factor_a_id, factor_b_id, as_of_date DESC
                """
            )
            return [
                (a, b, float(c))
                for a, b, c in cur.fetchall()
                if c is not None and float(c) <= threshold and a in by_id and b in by_id
            ]

    @staticmethod
    def _match_pure_noise(f: Dict[str, Any], t: Dict[str, float]) -> bool:
        if f.get("official_grade") != "D":
            return False
        ic = f.get("ind_ic")
        rank_ic = f.get("ind_rank_ic")
        pos = f.get("ic_positive_ratio")
        rank_icir = f.get("ind_rank_icir")
        if ic is None or rank_ic is None or pos is None or rank_icir is None:
            return False
        return (
            abs(float(ic)) < t["ic_th"]
            and abs(float(rank_ic)) < t["rank_ic_th"]
            and t["pos_ratio_lo"] <= float(pos) <= t["pos_ratio_hi"]
            and abs(float(rank_icir)) < t["rank_icir_th"]
        )

    @staticmethod
    def _resolve_reverse_pairs(
        neg_pairs: List[Tuple[int, int, float]],
        by_id: Dict[int, Dict[str, Any]],
        t: Dict[str, float],
    ) -> List[Tuple[int, int, float, str]]:
        """决策: 每对反向因子保留谁 / 禁用谁. 返回 (keep, drop, corr, reason)."""
        out: List[Tuple[int, int, float, str]] = []
        for a, b, c in neg_pairs:
            ic_a = by_id[a].get("ind_ic") or 0.0
            ic_b = by_id[b].get("ind_ic") or 0.0
            if c <= t["exact_neg"]:
                # 精确 -1: 优先留正 IC
                if ic_a >= 0 and ic_b < 0:
                    keep, drop = a, b
                elif ic_b >= 0 and ic_a < 0:
                    keep, drop = b, a
                else:
                    keep = a if abs(ic_a) >= abs(ic_b) else b
                    drop = b if keep == a else a
                reason = "corr=-1, 留正 IC"
            else:
                keep = a if abs(ic_a) >= abs(ic_b) else b
                drop = b if keep == a else a
                reason = "corr<-0.999, 留 |IC| 大"
            out.append((keep, drop, c, reason))
        return out


factor_cleanup_service = FactorCleanupService()
