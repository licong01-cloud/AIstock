from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
import math
from typing import Any, Callable, Mapping

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.multi_alpha.combine_ui_adapter import CombineUIAdapterError, task_key_for_run
from backend.services.multi_alpha.durable_models import (
    DurableChildSpec,
    DurableTaskSpec,
    artifact_manifest_hash_for,
    canonical_json,
    make_child_id,
    make_legacy_task_id,
    sha256_identity,
)
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepositoryError


ConnectionProvider = Callable[[], AbstractContextManager[Any]]


def _transaction_connection() -> AbstractContextManager[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


@dataclass(frozen=True)
class LegacyRunAssignment:
    run_id: str
    task_id: str
    legacy_group_key: str


@dataclass(frozen=True)
class LegacyBackfillPlan:
    tasks: tuple[DurableTaskSpec, ...]
    assignments: tuple[LegacyRunAssignment, ...]
    children: tuple[DurableChildSpec, ...]
    protected_digest: str

    def summary(self) -> dict[str, Any]:
        return {
            "task_count": len(self.tasks),
            "run_assignment_count": len(self.assignments),
            "child_count": len(self.children),
            "scheme_child_count": sum(child.child_kind == "scheme" for child in self.children),
            "loo_child_count": sum(child.child_kind == "loo" for child in self.children),
            "protected_digest": self.protected_digest,
            "task_ids": [task.task_id for task in self.tasks],
            "run_ids": [assignment.run_id for assignment in self.assignments],
        }


class MultiAlphaLegacyBackfill:
    """Idempotent historical task/run association and result-child mapping.

    It never fabricates remote attempts and never changes historical metrics,
    status, reason, created_at, or Archive data.
    """

    def __init__(self, connection_provider: ConnectionProvider = _transaction_connection) -> None:
        self._connection_provider = connection_provider

    def dry_run(self) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                plan = self._build_plan(cur)
                return {"mode": "dry-run", **plan.summary()}

    def execute(self) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                plan = self._build_plan(cur)
                before = plan.protected_digest
                for task in plan.tasks:
                    self._upsert_task(cur, task)
                for assignment in plan.assignments:
                    self._assign_run(cur, assignment)
                for child in plan.children:
                    self._upsert_child(cur, child)

                after = self._protected_digest(cur)
                if before != after:
                    raise MultiAlphaDurableRepositoryError(
                        "historical protected data changed during durable backfill",
                        reason_code="multi_alpha_backfill_protected_data_changed",
                        context={"before": before, "after": after},
                    )
                readback = self._readback(cur, plan)
                if not readback["ready"]:
                    raise MultiAlphaDurableRepositoryError(
                        "historical durable backfill readback did not match its plan",
                        reason_code="multi_alpha_backfill_readback_mismatch",
                        context=readback,
                    )
                return {"mode": "execute", **plan.summary(), "readback": readback}

    def readback(self) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                plan = self._build_plan(cur)
                return {"mode": "readback", **plan.summary(), "readback": self._readback(cur, plan)}

    def _build_plan(self, cur: Any) -> LegacyBackfillPlan:
        cur.execute(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_run
            ORDER BY created_at, id
            """
        )
        runs = [dict(row) for row in cur.fetchall()]
        run_ids = [str(run["id"]) for run in runs]
        if run_ids:
            cur.execute(
                """
                SELECT *
                FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result
                WHERE run_id = ANY(%s)
                ORDER BY run_id, weighting_scheme
                """,
                (run_ids,),
            )
            schemes = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT *
                FROM strategy_pkg.multi_alpha_combine_backtest_loo
                WHERE run_id = ANY(%s)
                ORDER BY run_id, weighting_scheme, dropped_leg_id
                """,
                (run_ids,),
            )
            loo_rows = [dict(row) for row in cur.fetchall()]
        else:
            schemes = []
            loo_rows = []
        return self.compile_plan(
            runs=runs,
            schemes=schemes,
            loo_rows=loo_rows,
            protected_digest=self._protected_digest(cur),
        )

    @staticmethod
    def compile_plan(
        *,
        runs: list[dict[str, Any]],
        schemes: list[dict[str, Any]],
        loo_rows: list[dict[str, Any]],
        protected_digest: str,
    ) -> LegacyBackfillPlan:
        runs = sorted(runs, key=lambda row: (str(row.get("created_at") or ""), str(row.get("id") or "")))
        schemes = sorted(schemes, key=lambda row: (str(row.get("run_id") or ""), str(row.get("weighting_scheme") or "")))
        loo_rows = sorted(
            loo_rows,
            key=lambda row: (
                str(row.get("run_id") or ""),
                str(row.get("weighting_scheme") or ""),
                str(row.get("dropped_leg_id") or ""),
            ),
        )
        groups: dict[str, list[dict[str, Any]]] = {}
        for run in runs:
            try:
                group_key = task_key_for_run(run)
            except CombineUIAdapterError as exc:
                raise MultiAlphaDurableRepositoryError(
                    "historical run cannot be mapped to its legacy task key",
                    reason_code="multi_alpha_backfill_invalid_legacy_run",
                    context={"run_id": run.get("id"), **dict(exc.context)},
                ) from exc
            groups.setdefault(group_key, []).append(run)

        tasks: list[DurableTaskSpec] = []
        assignments: list[LegacyRunAssignment] = []
        for group_key in sorted(groups):
            grouped_runs = groups[group_key]
            seed = grouped_runs[0]
            task_id = make_legacy_task_id(group_key)
            roster = _as_list_of_mappings(seed.get("roster_json"), field="roster_json", run_id=str(seed.get("id")))
            task_name = _legacy_task_name(seed, roster)
            default_request = {
                "roster": roster,
                "normalize_method": seed.get("normalize_method"),
                "walk_forward": _as_mapping(seed.get("walk_forward_json")),
                "backtest_config": _as_mapping(seed.get("backtest_config_json")),
                "baseline_leg_id": seed.get("baseline_leg_id"),
            }
            tasks.append(
                DurableTaskSpec(
                    task_id=task_id,
                    task_name=task_name,
                    roster_hash=str(seed.get("roster_hash") or ""),
                    roster=roster,
                    default_request=default_request,
                    source_kind="legacy_backfill",
                    description="Historical multi-alpha task reconstructed from the existing task_key_for_run identity.",
                    legacy_group_key=group_key,
                    created_by="durable_backfill_20260718",
                )
            )
            for run in grouped_runs:
                existing_task_id = run.get("task_id")
                if existing_task_id not in (None, task_id):
                    raise MultiAlphaDurableRepositoryError(
                        "historical run is already assigned to a different durable task",
                        reason_code="multi_alpha_backfill_task_assignment_conflict",
                        context={"run_id": run.get("id"), "expected_task_id": task_id, "actual_task_id": existing_task_id},
                    )
                assignments.append(
                    LegacyRunAssignment(run_id=str(run["id"]), task_id=task_id, legacy_group_key=group_key)
                )

        children = MultiAlphaLegacyBackfill._result_children(runs, schemes, loo_rows)
        return LegacyBackfillPlan(
            tasks=tuple(tasks),
            assignments=tuple(assignments),
            children=tuple(children),
            protected_digest=protected_digest,
        )

    @staticmethod
    def _result_children(
        runs: list[dict[str, Any]],
        schemes: list[dict[str, Any]],
        loo_rows: list[dict[str, Any]],
    ) -> list[DurableChildSpec]:
        run_ids = [str(run["id"]) for run in runs]
        if not run_ids:
            return []

        children: list[DurableChildSpec] = []
        ordinals: dict[str, int] = {run_id: 0 for run_id in run_ids}
        for row in schemes:
            run_id = str(row["run_id"])
            scheme = str(row["weighting_scheme"])
            child_key = f"scheme:{scheme}"
            manifest = {
                "source": "legacy_result_backfill",
                "run_id": run_id,
                "child_kind": "scheme",
                "weighting_scheme": scheme,
                "result_identity": {"table": "multi_alpha_combine_backtest_scheme_result", "id": row.get("id")},
            }
            children.append(
                DurableChildSpec(
                    child_id=make_child_id(run_id, child_key),
                    run_id=run_id,
                    child_key=child_key,
                    child_kind="scheme",
                    weighting_scheme=scheme,
                    ordinal=ordinals[run_id],
                    status="not_computable" if bool(row.get("skipped")) else "succeeded",
                    input_manifest=manifest,
                    input_manifest_hash=artifact_manifest_hash_for(manifest),
                    source_kind="legacy_result_backfill",
                )
            )
            ordinals[run_id] += 1

        for row in loo_rows:
            run_id = str(row["run_id"])
            scheme = str(row["weighting_scheme"])
            dropped_leg_id = str(row["dropped_leg_id"])
            child_key = f"loo:{scheme}:drop:{dropped_leg_id}"
            manifest = {
                "source": "legacy_result_backfill",
                "run_id": run_id,
                "child_kind": "loo",
                "weighting_scheme": scheme,
                "dropped_leg_id": dropped_leg_id,
                "result_identity": {"table": "multi_alpha_combine_backtest_loo", "id": row.get("id")},
            }
            children.append(
                DurableChildSpec(
                    child_id=make_child_id(run_id, child_key),
                    run_id=run_id,
                    child_key=child_key,
                    child_kind="loo",
                    weighting_scheme=scheme,
                    dropped_leg_id=dropped_leg_id,
                    ordinal=ordinals[run_id],
                    status="succeeded",
                    input_manifest=manifest,
                    input_manifest_hash=artifact_manifest_hash_for(manifest),
                    source_kind="legacy_result_backfill",
                )
            )
            ordinals[run_id] += 1
        return children

    def _upsert_task(self, cur: Any, task: DurableTaskSpec) -> None:
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_task
                (task_id, task_name, task_type, description, roster_hash, roster_json,
                 default_request_json, legacy_group_key, source_kind, created_by)
            VALUES (%s, %s, 'multi_alpha_combine', %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                task.task_id,
                task.task_name,
                task.description,
                task.roster_hash,
                Json(list(task.roster)),
                Json(dict(task.default_request)),
                task.legacy_group_key,
                task.source_kind,
                task.created_by,
            ),
        )
        cur.execute(
            """
            SELECT task_id, roster_hash, roster_json, default_request_json, legacy_group_key
            FROM strategy_pkg.multi_alpha_combine_task
            WHERE task_id = %s
            """,
            (task.task_id,),
        )
        row = cur.fetchone()
        if not row:
            raise MultiAlphaDurableRepositoryError(
                "legacy task insert was not readable",
                reason_code="multi_alpha_backfill_task_missing",
                context={"task_id": task.task_id},
            )
        existing = dict(row)
        expected = {
            "task_id": task.task_id,
            "roster_hash": task.roster_hash,
            "roster_json": canonical_json(list(task.roster)),
            "default_request_json": canonical_json(dict(task.default_request)),
            "legacy_group_key": task.legacy_group_key,
        }
        actual = {
            "task_id": existing.get("task_id"),
            "roster_hash": existing.get("roster_hash"),
            "roster_json": canonical_json(existing.get("roster_json")),
            "default_request_json": canonical_json(existing.get("default_request_json")),
            "legacy_group_key": existing.get("legacy_group_key"),
        }
        if expected != actual:
            raise MultiAlphaDurableRepositoryError(
                "legacy task identity maps to different frozen input",
                reason_code="multi_alpha_identity_payload_conflict",
                context={"task_id": task.task_id, "expected": expected, "actual": actual},
            )

    @staticmethod
    def _assign_run(cur: Any, assignment: LegacyRunAssignment) -> None:
        cur.execute(
            """
            UPDATE strategy_pkg.multi_alpha_combine_backtest_run
            SET task_id = %s,
                updated_at = CASE WHEN task_id IS DISTINCT FROM %s THEN NOW() ELSE updated_at END
            WHERE id = %s AND (task_id IS NULL OR task_id = %s)
            RETURNING id, task_id
            """,
            (assignment.task_id, assignment.task_id, assignment.run_id, assignment.task_id),
        )
        row = cur.fetchone()
        if not row:
            raise MultiAlphaDurableRepositoryError(
                "historical run task assignment conflicted",
                reason_code="multi_alpha_backfill_task_assignment_conflict",
                context=asdict(assignment),
            )

    @staticmethod
    def _upsert_child(cur: Any, child: DurableChildSpec) -> None:
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child
                (child_id, run_id, child_key, child_kind, weighting_scheme, dropped_leg_id,
                 ordinal, status, input_manifest_json, input_manifest_hash, source_kind, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT DO NOTHING
            """,
            (
                child.child_id,
                child.run_id,
                child.child_key,
                child.child_kind,
                child.weighting_scheme,
                child.dropped_leg_id,
                child.ordinal,
                child.status,
                Json(dict(child.input_manifest)),
                child.input_manifest_hash,
                child.source_kind,
            ),
        )
        cur.execute(
            """
            SELECT child_id, run_id, child_key, input_manifest_hash, source_kind, status
            FROM strategy_pkg.multi_alpha_combine_backtest_child
            WHERE child_id = %s
            """,
            (child.child_id,),
        )
        row = cur.fetchone()
        if not row:
            raise MultiAlphaDurableRepositoryError(
                "legacy result child insert was not readable",
                reason_code="multi_alpha_backfill_child_missing",
                context={"child_id": child.child_id},
            )
        existing = dict(row)
        expected = {
            "child_id": child.child_id,
            "run_id": child.run_id,
            "child_key": child.child_key,
            "input_manifest_hash": child.input_manifest_hash,
            "source_kind": child.source_kind,
            "status": child.status,
        }
        actual = {key: existing.get(key) for key in expected}
        if expected != actual:
            raise MultiAlphaDurableRepositoryError(
                "legacy child identity maps to different result input",
                reason_code="multi_alpha_identity_payload_conflict",
                context={"child_id": child.child_id, "expected": expected, "actual": actual},
            )

    def _readback(self, cur: Any, plan: LegacyBackfillPlan) -> dict[str, Any]:
        assignment_mismatches: list[dict[str, Any]] = []
        for assignment in plan.assignments:
            cur.execute(
                "SELECT task_id FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s",
                (assignment.run_id,),
            )
            row = cur.fetchone()
            actual = dict(row).get("task_id") if row else None
            if actual != assignment.task_id:
                assignment_mismatches.append(
                    {"run_id": assignment.run_id, "expected_task_id": assignment.task_id, "actual_task_id": actual}
                )

        missing_tasks: list[str] = []
        for task in plan.tasks:
            cur.execute(
                "SELECT 1 FROM strategy_pkg.multi_alpha_combine_task WHERE task_id = %s AND legacy_group_key = %s",
                (task.task_id, task.legacy_group_key),
            )
            if cur.fetchone() is None:
                missing_tasks.append(task.task_id)

        child_mismatches: list[dict[str, Any]] = []
        for child in plan.children:
            cur.execute(
                """
                SELECT input_manifest_hash, source_kind, status
                FROM strategy_pkg.multi_alpha_combine_backtest_child
                WHERE child_id = %s
                """,
                (child.child_id,),
            )
            row = cur.fetchone()
            actual = dict(row) if row else None
            expected = {
                "input_manifest_hash": child.input_manifest_hash,
                "source_kind": "legacy_result_backfill",
                "status": child.status,
            }
            if actual != expected:
                child_mismatches.append({"child_id": child.child_id, "expected": expected, "actual": actual})

        cur.execute(
            """
            SELECT COUNT(*) AS attempt_count
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
              ON child.child_id = attempt.child_id
            WHERE child.source_kind = 'legacy_result_backfill'
            """
        )
        attempt_count = int(dict(cur.fetchone() or {}).get("attempt_count") or 0)
        protected_digest = self._protected_digest(cur)
        protected_unchanged = protected_digest == plan.protected_digest
        return {
            "ready": not missing_tasks
            and not assignment_mismatches
            and not child_mismatches
            and attempt_count == 0
            and protected_unchanged,
            "missing_tasks": missing_tasks,
            "assignment_mismatches": assignment_mismatches,
            "child_mismatches": child_mismatches,
            "legacy_attempt_count": attempt_count,
            "protected_digest": protected_digest,
            "protected_unchanged": protected_unchanged,
        }

    @staticmethod
    def _protected_digest(cur: Any) -> str:
        cur.execute("SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run ORDER BY id")
        runs: list[dict[str, Any]] = []
        for raw in cur.fetchall():
            row = dict(raw)
            row.pop("task_id", None)
            row.pop("updated_at", None)
            runs.append(row)
        cur.execute(
            "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result ORDER BY run_id, weighting_scheme"
        )
        schemes = [dict(row) for row in cur.fetchall()]
        cur.execute(
            "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_loo ORDER BY run_id, weighting_scheme, dropped_leg_id"
        )
        loo_rows = [dict(row) for row in cur.fetchall()]
        return sha256_identity(_normalize_protected_value({"runs": runs, "scheme_results": schemes, "loo": loo_rows}))


def _as_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise MultiAlphaDurableRepositoryError(
            "historical JSON object has invalid shape",
            reason_code="multi_alpha_backfill_invalid_legacy_run",
            context={"type": type(value).__name__},
        )
    return dict(value)


def _as_list_of_mappings(value: Any, *, field: str, run_id: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise MultiAlphaDurableRepositoryError(
            "historical roster has invalid shape",
            reason_code="multi_alpha_backfill_invalid_legacy_run",
            context={"run_id": run_id, "field": field, "type": type(value).__name__},
        )
    return [dict(item) for item in value]


def _legacy_task_name(run: Mapping[str, Any], roster: list[dict[str, Any]]) -> str:
    leg_ids = [str(item.get("leg_id") or item.get("id") or "").strip() for item in roster]
    leg_ids = [item for item in leg_ids if item]
    roster_label = ", ".join(leg_ids[:4]) if leg_ids else str(run.get("roster_hash") or "")[:12]
    if len(leg_ids) > 4:
        roster_label += f" +{len(leg_ids) - 4}"
    return f"Legacy multi-alpha · {roster_label}"


def _normalize_protected_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_protected_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_protected_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return {"__nonfinite_float__": "nan"}
        return {"__nonfinite_float__": "infinity" if value > 0 else "-infinity"}
    if isinstance(value, memoryview):
        return {"__bytes__": bytes(value).hex()}
    return value
