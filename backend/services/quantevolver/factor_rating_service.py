from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from ...db.pg_pool import get_conn
from .factor_analyst import classify_holding_period
from .llm_client import get_llm_kwargs

logger = logging.getLogger("aistock.quantevolver.factor_rating_service")

_SCHEMA_READY = False
_RULES_SYNCED = False


class FactorRatingService:
    RULES_ROOT = Path(__file__).resolve().parents[2] / "rating_rules" / "factor"
    INDEX_FILE = RULES_ROOT / "index.json"

    def ensure_schema(self) -> None:
        global _SCHEMA_READY
        if _SCHEMA_READY:
            return

        ddl = [
            """
            CREATE TABLE IF NOT EXISTS qe_rating_rule_versions (
                rule_version TEXT PRIMARY KEY,
                version_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                rule_file_path TEXT NOT NULL,
                description_md TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                activated_at TIMESTAMPTZ
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS qe_factor_rating_runs (
                run_id TEXT PRIMARY KEY,
                rule_version TEXT NOT NULL REFERENCES qe_rating_rule_versions(rule_version) ON DELETE RESTRICT,
                scope_type TEXT NOT NULL,
                scope_payload JSONB,
                snapshot_date DATE,
                triggered_from TEXT NOT NULL DEFAULT 'ui_toolbar',
                status TEXT NOT NULL DEFAULT 'pending',
                summary JSONB,
                error_message TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS qe_factor_official_ratings (
                id BIGSERIAL PRIMARY KEY,
                factor_catalog_id BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                rule_version TEXT NOT NULL REFERENCES qe_rating_rule_versions(rule_version) ON DELETE RESTRICT,
                run_id TEXT NOT NULL REFERENCES qe_factor_rating_runs(run_id) ON DELETE CASCADE,
                snapshot_date DATE,
                official_score DOUBLE PRECISION NOT NULL,
                official_grade TEXT NOT NULL,
                dimension_scores JSONB NOT NULL DEFAULT '{}'::jsonb,
                hard_gate_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
                grade_reason_structured JSONB NOT NULL DEFAULT '{}'::jsonb,
                metrics_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
                llm_audit_summary TEXT,
                llm_risk_notes JSONB,
                graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (factor_catalog_id, rule_version, snapshot_date)
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_qe_factor_official_ratings_factor_version ON qe_factor_official_ratings(factor_catalog_id, rule_version);",
            "CREATE INDEX IF NOT EXISTS idx_qe_factor_official_ratings_grade ON qe_factor_official_ratings(rule_version, official_grade);",
            "CREATE INDEX IF NOT EXISTS idx_qe_factor_rating_runs_started_at ON qe_factor_rating_runs(started_at DESC);",
        ]
        with get_conn() as conn:
            with conn.cursor() as cur:
                for sql in ddl:
                    cur.execute(sql)
        _SCHEMA_READY = True

    def sync_rule_versions(self, force: bool = False) -> None:
        global _RULES_SYNCED
        self.ensure_schema()
        if _RULES_SYNCED and not force:
            return

        index = self._read_index()
        active_version = index.get("active_version")
        default_version = index.get("default_version") or active_version
        versions = index.get("versions") or []

        with get_conn() as conn:
            with conn.cursor() as cur:
                for item in versions:
                    version = str(item.get("version") or "").strip()
                    if not version:
                        continue
                    rule_dir = self.RULES_ROOT / version
                    readme_path = rule_dir / "README.md"
                    description_md = readme_path.read_text(encoding="utf-8") if readme_path.exists() else (item.get("description") or "")
                    status = "active" if version == active_version else (item.get("status") or ("default" if version == default_version else "draft"))
                    version_name = item.get("label") or item.get("version_name") or version
                    cur.execute(
                        """
                        INSERT INTO qe_rating_rule_versions (rule_version, version_name, status, rule_file_path, description_md, created_at, activated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW(), CASE WHEN %s = 'active' THEN NOW() ELSE NULL END)
                        ON CONFLICT (rule_version) DO UPDATE SET
                            version_name = EXCLUDED.version_name,
                            status = EXCLUDED.status,
                            rule_file_path = EXCLUDED.rule_file_path,
                            description_md = EXCLUDED.description_md,
                            activated_at = CASE WHEN EXCLUDED.status = 'active' THEN NOW() ELSE qe_rating_rule_versions.activated_at END
                        """,
                        (version, version_name, status, str(rule_dir), description_md, status),
                    )
        _RULES_SYNCED = True

    def list_rule_versions(self) -> Dict[str, Any]:
        self.sync_rule_versions()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rule_version, version_name, status, rule_file_path, description_md, created_at, activated_at
                    FROM qe_rating_rule_versions
                    ORDER BY activated_at DESC NULLS LAST, created_at DESC, rule_version DESC
                    """
                )
                rows = cur.fetchall()
        items = [
            {
                "rule_version": row[0],
                "version_name": row[1],
                "status": row[2],
                "rule_file_path": row[3],
                "description_md": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "activated_at": row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]
        active = next((item["rule_version"] for item in items if item["status"] == "active"), None)
        default = next((item["rule_version"] for item in items if item["status"] in {"active", "default"}), None)
        return {"rules": items, "active_version": active, "default_version": default or active}

    def get_rule_detail(self, version: str) -> Dict[str, Any]:
        self.sync_rule_versions()
        rule_dir = self.RULES_ROOT / version
        if not rule_dir.exists():
            raise ValueError(f"规则版本不存在: {version}")
        spec = self._read_yaml(rule_dir / "rule_spec.yaml")
        bands = self._read_yaml(rule_dir / "grade_bands.yaml")
        readme = (rule_dir / "README.md").read_text(encoding="utf-8") if (rule_dir / "README.md").exists() else ""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT rule_version, version_name, status, description_md, activated_at FROM qe_rating_rule_versions WHERE rule_version = %s",
                    (version,),
                )
                row = cur.fetchone()
        return {
            "rule_version": version,
            "version_name": row[1] if row else version,
            "status": row[2] if row else "draft",
            "description_md": readme or (row[3] if row else ""),
            "activated_at": row[4].isoformat() if row and row[4] else None,
            "spec": spec,
            "grade_bands": bands,
            "rule_dir": str(rule_dir),
        }

    def activate_rule_version(self, version: str) -> Dict[str, Any]:
        self.sync_rule_versions(force=True)
        detail = self.get_rule_detail(version)
        index = self._read_index()
        index["active_version"] = version
        if not index.get("default_version"):
            index["default_version"] = version
        self._write_index(index)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_rating_rule_versions SET status = CASE WHEN rule_version = %s THEN 'active' ELSE 'draft' END", (version,))
                cur.execute("UPDATE qe_rating_rule_versions SET activated_at = NOW() WHERE rule_version = %s", (version,))
        return {"ok": True, "rule_version": version, "version_name": detail["version_name"]}

    def list_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        self.ensure_schema()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, rule_version, scope_type, scope_payload, snapshot_date, triggered_from,
                           status, summary, error_message, started_at, finished_at
                    FROM qe_factor_rating_runs
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "run_id": row[0],
                    "rule_version": row[1],
                    "scope_type": row[2],
                    "scope_payload": row[3],
                    "snapshot_date": row[4].isoformat() if row[4] else None,
                    "triggered_from": row[5],
                    "status": row[6],
                    "summary": row[7],
                    "error_message": row[8],
                    "started_at": row[9].isoformat() if row[9] else None,
                    "finished_at": row[10].isoformat() if row[10] else None,
                }
            )
        return items

    def list_results(self, rule_version: Optional[str] = None, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        self.sync_rule_versions()
        selected_version = rule_version or self.list_rule_versions().get("active_version")
        if not selected_version:
            return {"rule_version": None, "total": 0, "items": []}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM qe_factor_official_ratings r
                    WHERE r.rule_version = %s
                    """,
                    (selected_version,),
                )
                total = cur.fetchone()[0]
                cur.execute(
                    """
                    SELECT c.factor_name, c.source, r.official_grade, r.official_score,
                           r.rule_version, r.grade_reason_structured, r.hard_gate_flags,
                           r.llm_audit_summary, r.llm_risk_notes,
                           r.graded_at, r.snapshot_date
                    FROM qe_factor_official_ratings r
                    JOIN aistock_factor_catalog c ON c.id = r.factor_catalog_id
                    WHERE r.rule_version = %s
                    ORDER BY r.graded_at DESC, c.factor_name
                    LIMIT %s OFFSET %s
                    """,
                    (selected_version, limit, offset),
                )
                rows = cur.fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "factor_name": row[0],
                    "source": row[1],
                    "official_grade": row[2],
                    "official_score": row[3],
                    "rule_version": row[4],
                    "grade_reason_structured": row[5],
                    "hard_gate_flags": row[6],
                    "llm_audit_summary": row[7],
                    "llm_risk_notes": row[8],
                    "graded_at": row[9].isoformat() if row[9] else None,
                    "snapshot_date": row[10].isoformat() if row[10] else None,
                }
            )
        return {"rule_version": selected_version, "total": total, "items": items}

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        self.ensure_schema()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, rule_version, scope_type, scope_payload, snapshot_date, triggered_from,
                           status, summary, error_message, started_at, finished_at
                    FROM qe_factor_rating_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "rule_version": row[1],
            "scope_type": row[2],
            "scope_payload": row[3],
            "snapshot_date": row[4].isoformat() if row[4] else None,
            "triggered_from": row[5],
            "status": row[6],
            "summary": row[7],
            "error_message": row[8],
            "started_at": row[9].isoformat() if row[9] else None,
            "finished_at": row[10].isoformat() if row[10] else None,
        }

    def run_rating(
        self,
        rule_version: str,
        scope_type: str,
        scope_payload: Dict[str, Any],
        triggered_from: str = "ui_toolbar",
    ) -> Dict[str, Any]:
        self.sync_rule_versions()
        if triggered_from != "ui_toolbar":
            raise ValueError("正式评级只能由 UI 工具栏触发")
        rule = self.get_rule_detail(rule_version)
        run_id = str(uuid.uuid4())
        self._insert_run(run_id, rule_version, scope_type, scope_payload, triggered_from)

        success_count = 0
        failed_count = 0
        errors: List[Dict[str, str]] = []
        snapshot_dates: List[str] = []

        try:
            factors = self._resolve_scope(scope_type, scope_payload, rule_version)
            for factor in factors:
                try:
                    result = self._grade_factor(factor, rule)
                    snapshot_date = result.get("snapshot_date")
                    if snapshot_date:
                        snapshot_dates.append(snapshot_date)
                    self._upsert_official_rating(run_id, rule_version, factor["id"], result)
                    success_count += 1
                except Exception as exc:  # noqa: BLE001
                    failed_count += 1
                    logger.exception("因子正式评级失败: %s", factor.get("factor_name"))
                    errors.append({"factor_name": factor.get("factor_name", "?"), "error": str(exc)})
            summary = {
                "total_factors": len(factors),
                "success_count": success_count,
                "failed_count": failed_count,
                "snapshot_dates": sorted({d for d in snapshot_dates if d}),
                "errors": errors[:20],
            }
            self._finish_run(run_id, "completed", summary=summary)
            return {"ok": True, "run_id": run_id, **summary}
        except Exception as exc:  # noqa: BLE001
            logger.exception("正式评级运行失败")
            self._finish_run(run_id, "failed", summary={"success_count": success_count, "failed_count": failed_count}, error_message=str(exc))
            raise

    def _insert_run(
        self,
        run_id: str,
        rule_version: str,
        scope_type: str,
        scope_payload: Dict[str, Any],
        triggered_from: str,
    ) -> None:
        self.ensure_schema()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qe_factor_rating_runs (run_id, rule_version, scope_type, scope_payload, triggered_from, status)
                    VALUES (%s, %s, %s, %s::jsonb, %s, 'running')
                    """,
                    (run_id, rule_version, scope_type, json.dumps(scope_payload, ensure_ascii=False), triggered_from),
                )

    def _finish_run(
        self,
        run_id: str,
        status: str,
        summary: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_factor_rating_runs
                    SET status = %s,
                        summary = COALESCE(%s::jsonb, summary),
                        error_message = %s,
                        finished_at = NOW()
                    WHERE run_id = %s
                    """,
                    (status, json.dumps(summary, ensure_ascii=False) if summary is not None else None, error_message, run_id),
                )

    def _resolve_scope(self, scope_type: str, scope_payload: Dict[str, Any], rule_version: str) -> List[Dict[str, Any]]:
        if scope_type == "selected":
            return self._resolve_selected_factors(scope_payload.get("selected_factors") or [])
        if scope_type == "filter":
            return self._resolve_filtered_factors(scope_payload.get("filters") or {}, rule_version)
        if scope_type == "all":
            return self._resolve_filtered_factors({}, rule_version)
        raise ValueError(f"不支持的范围类型: {scope_type}")

    def _resolve_selected_factors(self, selected_factors: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        if not selected_factors:
            return []
        clauses: List[str] = []
        params: List[Any] = []
        for item in selected_factors:
            factor_name = str(item.get("factor_name") or "").strip()
            source = str(item.get("source") or "").strip()
            if not factor_name or not source:
                continue
            clauses.append("(factor_name = %s AND source = %s)")
            params.extend([factor_name, source])
        if not clauses:
            return []
        sql = f"SELECT id, factor_name, source FROM aistock_factor_catalog WHERE {' OR '.join(clauses)} ORDER BY factor_name, source"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [{"id": row[0], "factor_name": row[1], "source": row[2]} for row in rows]

    def _resolve_filtered_factors(self, filters: Dict[str, Any], rule_version: str) -> List[Dict[str, Any]]:
        conditions: List[str] = []
        params: List[Any] = [rule_version]
        category_conditions: List[str] = []
        grade_conditions: List[str] = []

        source = str(filters.get("source") or "").strip()
        if source:
            conditions.append("c.source = %s")
            params.append(source)

        exclude_source = str(filters.get("exclude_source") or "").strip()
        if exclude_source:
            ex_list = [s.strip() for s in exclude_source.split(",") if s.strip()]
            if ex_list:
                placeholders = ",".join(["%s"] * len(ex_list))
                conditions.append(f"c.source NOT IN ({placeholders})")
                params.extend(ex_list)

        search = str(filters.get("search") or "").strip()
        if search:
            conditions.append("c.factor_name ILIKE %s")
            params.append(f"%{search}%")

        availability = str(filters.get("availability") or "").strip()
        if availability == "enabled":
            conditions.append("c.is_available = TRUE")
        elif availability == "disabled":
            conditions.append("c.is_available = FALSE")

        category = str(filters.get("category") or "").strip()
        if category == "__empty__":
            category_conditions.append("cl.category IS NULL")
        elif category:
            category_conditions.append("cl.category = %s")
            params.append(category)

        grade = str(filters.get("grade") or "").strip()
        if grade == "__empty__":
            grade_conditions.append("fr.official_grade IS NULL")
        elif grade:
            grade_conditions.append("fr.official_grade = %s")
            params.append(grade)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        extra_conditions = ""
        combined_extra = category_conditions + grade_conditions
        if combined_extra:
            extra_conditions = " AND " + " AND ".join(combined_extra)

        sql = f"""
            WITH selected_rule AS (SELECT %s::text AS rule_version)
            SELECT c.id, c.factor_name, c.source
            FROM aistock_factor_catalog c
            LEFT JOIN qe_factor_classification cl
              ON cl.factor_name = c.factor_name AND cl.factor_source = c.source
            LEFT JOIN LATERAL (
                SELECT official_grade
                FROM qe_factor_official_ratings r
                WHERE r.factor_catalog_id = c.id
                  AND r.rule_version = (SELECT rule_version FROM selected_rule)
                ORDER BY r.graded_at DESC
                LIMIT 1
            ) fr ON TRUE
            WHERE {where_clause}{extra_conditions}
            ORDER BY c.factor_name, c.source
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [{"id": row[0], "factor_name": row[1], "source": row[2]} for row in rows]

    def _grade_factor(self, factor: Dict[str, Any], rule: Dict[str, Any]) -> Dict[str, Any]:
        factor_name = factor["factor_name"]
        factor_source = factor["source"]
        metrics_by_window = self._fetch_metrics_by_window(factor_name)
        classification_meta = self._fetch_classification_meta(factor_name, factor_source)
        full_metrics = metrics_by_window.get("full") or {}

        holding_period_class = classification_meta.get("holding_period_class") or classify_holding_period(full_metrics.get("ic_decay_half_life"))
        core_ic = self._compute_core_ic(full_metrics, holding_period_class)
        spec = rule["spec"]

        dimension_scores = {
            "predictive_strength": self._score_predictive_strength(full_metrics, holding_period_class, spec),
            "stability": self._score_stability(metrics_by_window, full_metrics, holding_period_class, spec),
            "economic_quality": self._score_economic_quality(full_metrics, spec),
            "selection_stability_cost": self._score_selection_stability_cost(full_metrics, holding_period_class, spec),
            "monotonicity_reliability": self._score_monotonicity_reliability(full_metrics, spec),
            "multi_alpha_fitness": self._score_multi_alpha_fitness(classification_meta, spec),
        }
        total_score = round(sum(dimension_scores.values()), 2)

        hard_gates = self._evaluate_hard_gates(metrics_by_window, full_metrics, core_ic, spec)
        grade = self._assign_grade(total_score, hard_gates, rule["grade_bands"])
        summary_text = self._build_summary_text(dimension_scores, hard_gates, total_score, grade)
        llm_review = self._run_llm_audit(factor_name, factor_source, rule, total_score, grade, dimension_scores, hard_gates, metrics_by_window, classification_meta)

        snapshot_date = full_metrics.get("snapshot_date") or full_metrics.get("data_end")
        snapshot_date = str(snapshot_date) if snapshot_date is not None else None

        return {
            "official_score": total_score,
            "official_grade": grade,
            "dimension_scores": dimension_scores,
            "hard_gate_flags": hard_gates,
            "grade_reason_structured": {
                "summary": summary_text,
                "holding_period_class": holding_period_class,
                "core_ic": core_ic,
                "failed_gates": [k for k, v in hard_gates.items() if v is False],
            },
            "metrics_snapshot": {
                "full": full_metrics,
                "out_sample": metrics_by_window.get("out_sample"),
                "recent_6m": metrics_by_window.get("recent_6m"),
                "recent_3m": metrics_by_window.get("recent_3m"),
                "classification_meta": classification_meta,
            },
            "llm_audit_summary": llm_review.get("summary") if llm_review else None,
            "llm_risk_notes": llm_review.get("risk_notes") if llm_review else None,
            "snapshot_date": snapshot_date,
        }

    def _fetch_metrics_by_window(self, factor_name: str) -> Dict[str, Dict[str, Any]]:
        windows = ["full", "out_sample", "recent_6m", "recent_3m"]
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (eval_window)
                           eval_window, ic_mean, rank_ic_mean, icir, rank_icir,
                           icir_annualized, rank_icir_annualized, ic_positive_ratio,
                           top_annual_return, top_excess_annual_return, top_sharpe,
                           top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
                           group_return_monotonicity, turnover, ic_decay_half_life,
                           coverage, n_trading_days, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
                           data_start, data_end, snapshot_date, calculated_at
                    FROM aistock_factor_metrics
                    WHERE factor_name = %s
                      AND eval_window = ANY(%s)
                    ORDER BY eval_window, calculated_at DESC
                    """,
                    (factor_name, windows),
                )
                rows = cur.fetchall()
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            result[row[0]] = {
                "ic_mean": row[1],
                "rank_ic_mean": row[2],
                "icir": row[3],
                "rank_icir": row[4],
                "icir_annualized": row[5],
                "rank_icir_annualized": row[6],
                "ic_positive_ratio": row[7],
                "top_annual_return": row[8],
                "top_excess_annual_return": row[9],
                "top_sharpe": row[10],
                "top_max_drawdown": row[11],
                "top_excess_sharpe": row[12],
                "benchmark_annual_return": row[13],
                "group_return_monotonicity": row[14],
                "turnover": row[15],
                "ic_decay_half_life": row[16],
                "coverage": row[17],
                "n_trading_days": row[18],
                "rank_ic_1d": row[19],
                "rank_ic_5d": row[20],
                "rank_ic_10d": row[21],
                "rank_ic_20d": row[22],
                "data_start": row[23],
                "data_end": row[24],
                "snapshot_date": row[25],
                "calculated_at": row[26],
            }
        return result

    def _fetch_classification_meta(self, factor_name: str, factor_source: str) -> Dict[str, Any]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT category, factor_dimension, holding_period_class,
                               data_source_group, linearity, ts_info_density
                        FROM qe_factor_classification
                        WHERE factor_name = %s AND factor_source = %s
                        LIMIT 1
                        """,
                        (factor_name, factor_source),
                    )
                    row = cur.fetchone()
        except Exception:
            row = None
        if not row:
            return {}
        return {
            "category": row[0],
            "factor_dimension": row[1],
            "holding_period_class": row[2],
            "data_source_group": row[3],
            "linearity": row[4],
            "ts_info_density": row[5],
        }

    def _score_predictive_strength(self, full_metrics: Dict[str, Any], holding_period_class: str, spec: Dict[str, Any]) -> float:
        thresholds = spec["thresholds"]
        score = 0.0
        score += self._score_higher_better(
            self._compute_core_ic(full_metrics, holding_period_class),
            thresholds["core_ic"],
            15.0,
            absolute=True,
        )
        score += self._score_higher_better(full_metrics.get("rank_ic_mean"), thresholds["rank_ic_mean_abs"], 5.0, absolute=True)
        horizon_vals = [full_metrics.get(k) for k in ("rank_ic_1d", "rank_ic_5d", "rank_ic_10d", "rank_ic_20d") if full_metrics.get(k) is not None]
        horizon_strength = sum(abs(float(v)) for v in horizon_vals) / len(horizon_vals) if horizon_vals else None
        score += self._score_higher_better(horizon_strength, thresholds["core_ic"], 5.0)
        return round(score, 2)

    def _score_stability(self, metrics_by_window: Dict[str, Dict[str, Any]], full_metrics: Dict[str, Any], holding_period_class: str, spec: Dict[str, Any]) -> float:
        thresholds = spec["thresholds"]
        score = 0.0
        score += self._score_higher_better(full_metrics.get("icir_annualized"), thresholds["icir_annualized"], 10.0, absolute=True)
        score += self._score_higher_better(full_metrics.get("rank_icir_annualized"), thresholds["rank_icir_annualized"], 5.0, absolute=True)
        score += self._score_higher_better(full_metrics.get("ic_positive_ratio"), thresholds["ic_positive_ratio"], 5.0)
        score += self._score_window_consistency(metrics_by_window, holding_period_class, 5.0)
        return round(score, 2)

    def _score_economic_quality(self, full_metrics: Dict[str, Any], spec: Dict[str, Any]) -> float:
        thresholds = spec["thresholds"]
        score = 0.0
        score += self._score_higher_better(full_metrics.get("top_excess_annual_return"), thresholds["top_excess_annual_return"], 7.0)
        score += self._score_higher_better(full_metrics.get("top_excess_sharpe"), thresholds["top_excess_sharpe"], 5.0, absolute=True)
        score += self._score_lower_better(full_metrics.get("top_max_drawdown"), thresholds["top_max_drawdown_abs"], 3.0, absolute=True)
        return round(score, 2)

    def _score_selection_stability_cost(self, full_metrics: Dict[str, Any], holding_period_class: str, spec: Dict[str, Any]) -> float:
        thresholds = spec["thresholds"]
        score = 0.0
        score += self._score_lower_better(full_metrics.get("turnover"), thresholds["turnover"], 10.0)
        score += self._score_higher_better(full_metrics.get("ic_decay_half_life"), thresholds["ic_decay_half_life"], 5.0)
        if holding_period_class == "long":
            score += 0.5
        elif holding_period_class == "medium":
            score += 0.25
        return round(min(score, 15.0), 2)

    def _score_monotonicity_reliability(self, full_metrics: Dict[str, Any], spec: Dict[str, Any]) -> float:
        thresholds = spec["thresholds"]
        score = 0.0
        score += self._score_higher_better(full_metrics.get("group_return_monotonicity"), thresholds["group_return_monotonicity"], 5.0)
        score += self._score_higher_better(full_metrics.get("coverage"), thresholds["coverage"], 3.0)
        score += self._score_higher_better(full_metrics.get("n_trading_days"), thresholds["n_trading_days"], 2.0)
        return round(score, 2)

    def _score_multi_alpha_fitness(self, classification_meta: Dict[str, Any], spec: Dict[str, Any]) -> float:  # noqa: ARG002
        score = 0.0
        if classification_meta.get("data_source_group") and classification_meta.get("data_source_group") != "unknown":
            score += 3.0
        if classification_meta.get("factor_dimension"):
            score += 2.0
        if classification_meta.get("linearity"):
            score += 2.0
        if classification_meta.get("holding_period_class") and classification_meta.get("holding_period_class") != "unknown":
            score += 2.0
        if classification_meta.get("ts_info_density"):
            score += 1.0
        return round(score, 2)

    def _evaluate_hard_gates(
        self,
        metrics_by_window: Dict[str, Dict[str, Any]],
        full_metrics: Dict[str, Any],
        core_ic: Optional[float],
        spec: Dict[str, Any],
    ) -> Dict[str, Any]:
        gates = spec.get("hard_gates", {})
        recent_neg_limit = float(gates.get("recent_negative_threshold", -0.005))
        recent_6m_core = self._compute_core_ic(metrics_by_window.get("recent_6m") or {}, classify_holding_period((metrics_by_window.get("recent_6m") or {}).get("ic_decay_half_life")))
        recent_3m_core = self._compute_core_ic(metrics_by_window.get("recent_3m") or {}, classify_holding_period((metrics_by_window.get("recent_3m") or {}).get("ic_decay_half_life")))
        both_recent_negative = bool(
            recent_6m_core is not None and recent_3m_core is not None and recent_6m_core < recent_neg_limit and recent_3m_core < recent_neg_limit
        )
        return {
            "s_core_ic": (core_ic or 0.0) >= float(gates["S"]["min_core_ic"]),
            "s_recent_ok": not both_recent_negative,
            "s_monotonicity": (full_metrics.get("group_return_monotonicity") or -999.0) > float(gates["S"]["min_monotonicity"]),
            "s_excess_ann": (full_metrics.get("top_excess_annual_return") or -999.0) > float(gates["S"]["min_excess_annual_return"]),
            "s_coverage": (full_metrics.get("coverage") or 0.0) >= float(gates["S"]["min_coverage"]),
            "s_turnover": (full_metrics.get("turnover") or 999.0) <= float(gates["S"]["max_turnover"]),
            "a_core_ic": (core_ic or 0.0) >= float(gates["A"]["min_core_ic"]),
            "a_recent_ok": not both_recent_negative,
            "a_monotonicity": (full_metrics.get("group_return_monotonicity") or -999.0) > float(gates["A"]["min_monotonicity"]),
            "a_coverage": (full_metrics.get("coverage") or 0.0) >= float(gates["A"]["min_coverage"]),
            "a_turnover": (full_metrics.get("turnover") or 999.0) <= float(gates["A"]["max_turnover"]),
        }

    def _assign_grade(self, score: float, hard_gates: Dict[str, Any], grade_bands: Dict[str, Any]) -> str:
        if score >= float(grade_bands["S"]["min_score"]) and all(
            hard_gates[k] for k in ("s_core_ic", "s_recent_ok", "s_monotonicity", "s_excess_ann", "s_coverage", "s_turnover")
        ):
            return "S"
        if score >= float(grade_bands["A"]["min_score"]) and all(
            hard_gates[k] for k in ("a_core_ic", "a_recent_ok", "a_monotonicity", "a_coverage", "a_turnover")
        ):
            return "A"
        if score >= float(grade_bands["B"]["min_score"]):
            return "B"
        if score >= float(grade_bands["C"]["min_score"]):
            return "C"
        return "D"

    def _upsert_official_rating(self, run_id: str, rule_version: str, factor_catalog_id: int, result: Dict[str, Any]) -> None:
        snapshot_date = result.get("snapshot_date")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qe_factor_official_ratings (
                        factor_catalog_id, rule_version, run_id, snapshot_date, official_score,
                        official_grade, dimension_scores, hard_gate_flags, grade_reason_structured,
                        metrics_snapshot, llm_audit_summary, llm_risk_notes, graded_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s::jsonb, %s::jsonb, %s::jsonb,
                        %s::jsonb, %s, %s::jsonb, NOW()
                    )
                    ON CONFLICT (factor_catalog_id, rule_version, snapshot_date) DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        official_score = EXCLUDED.official_score,
                        official_grade = EXCLUDED.official_grade,
                        dimension_scores = EXCLUDED.dimension_scores,
                        hard_gate_flags = EXCLUDED.hard_gate_flags,
                        grade_reason_structured = EXCLUDED.grade_reason_structured,
                        metrics_snapshot = EXCLUDED.metrics_snapshot,
                        llm_audit_summary = EXCLUDED.llm_audit_summary,
                        llm_risk_notes = EXCLUDED.llm_risk_notes,
                        graded_at = NOW()
                    """,
                    (
                        factor_catalog_id,
                        rule_version,
                        run_id,
                        snapshot_date,
                        result["official_score"],
                        result["official_grade"],
                        json.dumps(result["dimension_scores"], ensure_ascii=False),
                        json.dumps(result["hard_gate_flags"], ensure_ascii=False),
                        json.dumps(result["grade_reason_structured"], ensure_ascii=False),
                        json.dumps(result["metrics_snapshot"], ensure_ascii=False, default=str),
                        result.get("llm_audit_summary"),
                        json.dumps(result.get("llm_risk_notes"), ensure_ascii=False) if result.get("llm_risk_notes") is not None else None,
                    ),
                )

    def _run_llm_audit(
        self,
        factor_name: str,
        factor_source: str,
        rule: Dict[str, Any],
        total_score: float,
        grade: str,
        dimension_scores: Dict[str, float],
        hard_gates: Dict[str, Any],
        metrics_by_window: Dict[str, Dict[str, Any]],
        classification_meta: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        try:
            import litellm
        except ImportError:
            return None

        system_prompt = """你是 AIstock 因子评级审阅员。正式评级 official_grade 已由统一规则引擎给出，你不能修改正式等级，只能补充解释、风险提示和人工复核意见。请严格输出 JSON。"""
        user_prompt = json.dumps(
            {
                "factor_name": factor_name,
                "factor_source": factor_source,
                "rule_version": rule.get("rule_version"),
                "official_score": total_score,
                "official_grade": grade,
                "dimension_scores": dimension_scores,
                "hard_gate_flags": hard_gates,
                "metrics_by_window": metrics_by_window,
                "classification_meta": classification_meta,
                "instructions": {
                    "must_not_change_grade": True,
                    "focus": [
                        "解释为何得到当前正式评级",
                        "指出最主要的风险项",
                        "如有边界问题给出人工复核建议",
                    ],
                    "response_schema": {
                        "summary": "string",
                        "risk_notes": ["string"],
                    },
                },
            },
            ensure_ascii=False,
            default=str,
        )
        try:
            kwargs = get_llm_kwargs("factor_analyst")
            response = litellm.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
                max_tokens=600,
                response_format={"type": "json_object"},
                **kwargs,
            )
            content = response.choices[0].message.content.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            data = json.loads(content)
            summary = str(data.get("summary") or "").strip()
            risk_notes = data.get("risk_notes") if isinstance(data.get("risk_notes"), list) else []
            return {
                "summary": summary or None,
                "risk_notes": [str(x) for x in risk_notes if str(x).strip()],
            }
        except Exception as e:  # noqa: BLE001
            logger.warning("因子评级 LLM 审阅失败 (%s): %s", factor_name, e)
            return None

    def _compute_core_ic(self, metrics: Dict[str, Any], holding_period_class: str) -> Optional[float]:
        if not metrics:
            return None
        values: List[float] = []
        if metrics.get("ic_mean") is not None:
            values.append(abs(float(metrics["ic_mean"])))
        if holding_period_class == "short":
            if metrics.get("rank_ic_1d") is not None:
                values.append(abs(float(metrics["rank_ic_1d"])))
        elif holding_period_class == "medium":
            for key in ("rank_ic_5d", "rank_ic_10d"):
                if metrics.get(key) is not None:
                    values.append(abs(float(metrics[key])))
        elif holding_period_class == "long":
            if metrics.get("rank_ic_20d") is not None:
                values.append(abs(float(metrics["rank_ic_20d"])))
        if not values:
            return None
        return max(values)

    def _score_window_consistency(self, metrics_by_window: Dict[str, Dict[str, Any]], holding_period_class: str, max_points: float) -> float:
        full_core = self._compute_core_ic(metrics_by_window.get("full") or {}, holding_period_class)
        recent_6m_core = self._compute_core_ic(metrics_by_window.get("recent_6m") or {}, holding_period_class)
        recent_3m_core = self._compute_core_ic(metrics_by_window.get("recent_3m") or {}, holding_period_class)
        values = [v for v in (full_core, recent_6m_core, recent_3m_core) if v is not None]
        if len(values) <= 1:
            return max_points * 0.5
        avg = sum(values) / len(values)
        if avg <= 0:
            return 0.0
        spread = max(values) - min(values)
        ratio = max(0.0, 1.0 - (spread / (avg + 1e-8)))
        return round(max_points * min(ratio, 1.0), 2)

    def _score_higher_better(
        self,
        value: Optional[float],
        bands: Dict[str, float],
        max_points: float,
        absolute: bool = False,
    ) -> float:
        if value is None:
            return 0.0
        val = abs(float(value)) if absolute else float(value)
        weak = float(bands["weak"])
        excellent = float(bands["excellent"])
        if val <= weak:
            return 0.0
        if val >= excellent:
            return max_points
        return round((val - weak) / max(excellent - weak, 1e-8) * max_points, 2)

    def _score_lower_better(
        self,
        value: Optional[float],
        bands: Dict[str, float],
        max_points: float,
        absolute: bool = False,
    ) -> float:
        if value is None:
            return 0.0
        val = abs(float(value)) if absolute else float(value)
        excellent = float(bands["excellent"])
        weak = float(bands["weak"])
        if val <= excellent:
            return max_points
        if val >= weak:
            return 0.0
        return round((weak - val) / max(weak - excellent, 1e-8) * max_points, 2)

    def _build_summary_text(
        self,
        dimension_scores: Dict[str, float],
        hard_gates: Dict[str, Any],
        total_score: float,
        grade: str,
    ) -> str:
        parts = [
            f"正式评级 {grade} / {total_score:.2f} 分",
            f"预测强度 {dimension_scores['predictive_strength']:.2f}/25",
            f"稳定性 {dimension_scores['stability']:.2f}/25",
            f"经济质量 {dimension_scores['economic_quality']:.2f}/15",
            f"低换手与选股稳定性 {dimension_scores['selection_stability_cost']:.2f}/15",
            f"单调性与可靠性 {dimension_scores['monotonicity_reliability']:.2f}/10",
            f"Multi-Alpha 适配 {dimension_scores['multi_alpha_fitness']:.2f}/10",
        ]
        failed = [k for k, v in hard_gates.items() if v is False]
        if failed:
            parts.append("未通过硬门槛: " + ", ".join(failed))
        return "；".join(parts)

    def _read_index(self) -> Dict[str, Any]:
        if not self.INDEX_FILE.exists():
            raise ValueError(f"评级规则索引不存在: {self.INDEX_FILE}")
        return json.loads(self.INDEX_FILE.read_text(encoding="utf-8"))

    def _write_index(self, data: Dict[str, Any]) -> None:
        self.INDEX_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _read_yaml(path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise ValueError(f"规则文件不存在: {path}")
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}


factor_rating_service = FactorRatingService()
