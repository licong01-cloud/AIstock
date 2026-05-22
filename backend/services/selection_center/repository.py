"""Persistence for package-based Selection Center."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError

from .models import SelectionCandidate, SelectionExclusion, SelectionMode, SelectionPaperPortfolioLink, SelectionRun, SelectionRunStatus

ConnFactory = Callable[[], Iterator[Any]]


class SelectionCenterRepository:
    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def create_run(self, run: SelectionRun) -> SelectionRun:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO selection.run (
                        run_id, mode, trade_date, data_source, package_ids, runtime_config,
                        status, valid_no_candidate, no_candidate_reason, error_json,
                        created_at, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run.run_id,
                        run.mode.value,
                        run.trade_date,
                        run.data_source,
                        psycopg2.extras.Json(run.package_ids),
                        psycopg2.extras.Json(run.runtime_config),
                        run.status.value,
                        run.valid_no_candidate,
                        run.no_candidate_reason,
                        psycopg2.extras.Json(run.error) if run.error else None,
                        run.created_at,
                        run.completed_at,
                    ),
                )
        return run

    def complete_run(self, run: SelectionRun) -> SelectionRun:
        completed = run.model_copy(
            update={
                "status": SelectionRunStatus.VALID_NO_CANDIDATE if run.valid_no_candidate else SelectionRunStatus.SUCCEEDED,
                "completed_at": datetime.now(UTC),
            }
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE selection.run
                    SET status = %s, runtime_config = %s, valid_no_candidate = %s, no_candidate_reason = %s,
                        completed_at = %s
                    WHERE run_id = %s
                    """,
                    (
                        completed.status.value,
                        psycopg2.extras.Json(completed.runtime_config),
                        completed.valid_no_candidate,
                        completed.no_candidate_reason,
                        completed.completed_at,
                        completed.run_id,
                    ),
                )
                for package_id, candidates in completed.package_results.items():
                    manifest_sha = completed.manifest_sha256_by_package[package_id]
                    for candidate in candidates:
                        cur.execute(
                            """
                            INSERT INTO selection.package_result (
                                run_id, package_id, manifest_sha256, symbol, score, rank,
                                target_weight, target_quantity, reference_price,
                                component_scores, reason
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                completed.run_id,
                                package_id,
                                manifest_sha,
                                candidate.symbol,
                                candidate.score,
                                candidate.rank,
                                candidate.target_weight,
                                candidate.target_quantity,
                                candidate.reference_price,
                                psycopg2.extras.Json(candidate.component_scores),
                                candidate.reason,
                            ),
                        )
                for candidate in completed.aggregate_results:
                    source_package_ids = candidate.component_scores.get("source_package_ids") or completed.package_ids
                    cur.execute(
                        """
                        INSERT INTO selection.aggregate_result (
                            run_id, symbol, score, rank, target_weight, target_quantity,
                            reference_price, source_package_ids, explanation
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            completed.run_id,
                            candidate.symbol,
                            candidate.score,
                            candidate.rank,
                            candidate.target_weight,
                            candidate.target_quantity,
                            candidate.reference_price,
                            psycopg2.extras.Json(source_package_ids),
                            psycopg2.extras.Json(candidate.component_scores),
                        ),
                    )
                for package_id, exclusions in completed.excluded_results.items():
                    manifest_sha = completed.manifest_sha256_by_package[package_id]
                    for exclusion in exclusions:
                        cur.execute(
                            """
                            INSERT INTO selection.excluded_result (
                                run_id, package_id, manifest_sha256, symbol, score,
                                raw_rank, reason, source, context
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (run_id, package_id, symbol, reason) DO UPDATE SET
                                score = EXCLUDED.score,
                                raw_rank = EXCLUDED.raw_rank,
                                source = EXCLUDED.source,
                                context = EXCLUDED.context
                            """,
                            (
                                completed.run_id,
                                package_id,
                                manifest_sha,
                                exclusion.symbol,
                                exclusion.score,
                                exclusion.rank,
                                exclusion.reason,
                                exclusion.source,
                                psycopg2.extras.Json(exclusion.context),
                            ),
                        )
        return completed

    def fail_run(self, run: SelectionRun, error: dict[str, Any]) -> SelectionRun:
        failed = run.model_copy(
            update={"status": SelectionRunStatus.FAILED, "error": error, "completed_at": datetime.now(UTC)}
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE selection.run
                    SET status = %s, error_json = %s, completed_at = %s
                    WHERE run_id = %s
                    """,
                    (failed.status.value, psycopg2.extras.Json(error), failed.completed_at, failed.run_id),
                )
        return failed

    def list_runs(self, *, limit: int = 100) -> list[SelectionRun]:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id
                    FROM selection.run
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                run_ids = [row[0] for row in cur.fetchall()]
        return [self.get_run(run_id) for run_id in run_ids]

    def get_run(self, run_id: str) -> SelectionRun:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM selection.run WHERE run_id = %s", (run_id,))
                row = cur.fetchone()
                if not row:
                    raise DataUnavailableError("selection run does not exist", context={"run_id": run_id})
                cur.execute(
                    """
                    SELECT package_id, manifest_sha256, symbol, score, rank,
                           target_weight, target_quantity, reference_price,
                           component_scores, reason
                    FROM selection.package_result
                    WHERE run_id = %s
                    ORDER BY package_id, rank
                    """,
                    (run_id,),
                )
                package_rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT symbol, score, rank, target_weight, target_quantity,
                           reference_price, explanation
                    FROM selection.aggregate_result
                    WHERE run_id = %s
                    ORDER BY rank
                    """,
                    (run_id,),
                )
                aggregate_rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT package_id, manifest_sha256, symbol, score, raw_rank,
                           reason, source, context
                    FROM selection.excluded_result
                    WHERE run_id = %s
                    ORDER BY package_id, raw_rank
                    """,
                    (run_id,),
                )
                exclusion_rows = cur.fetchall()
        package_results: dict[str, list[SelectionCandidate]] = {}
        manifest_sha: dict[str, str] = {}
        for item in package_rows:
            package_id = item["package_id"]
            manifest_sha[package_id] = item["manifest_sha256"]
            package_results.setdefault(package_id, []).append(
                SelectionCandidate(
                    symbol=item["symbol"],
                    score=float(item["score"]),
                    rank=int(item["rank"]),
                    target_weight=float(item["target_weight"]) if item["target_weight"] is not None else None,
                    target_quantity=int(item["target_quantity"]) if item["target_quantity"] is not None else None,
                    reference_price=float(item["reference_price"]) if item["reference_price"] is not None else None,
                    component_scores=item["component_scores"] or {},
                    reason=item["reason"],
                )
            )
        aggregate = [
            SelectionCandidate(
                symbol=item["symbol"],
                score=float(item["score"]),
                rank=int(item["rank"]),
                target_weight=float(item["target_weight"]) if item["target_weight"] is not None else None,
                target_quantity=int(item["target_quantity"]) if item["target_quantity"] is not None else None,
                reference_price=float(item["reference_price"]) if item["reference_price"] is not None else None,
                component_scores=item["explanation"] or {},
            )
            for item in aggregate_rows
        ]
        excluded_results: dict[str, list[SelectionExclusion]] = {}
        for item in exclusion_rows:
            package_id = item["package_id"]
            manifest_sha[package_id] = item["manifest_sha256"]
            excluded_results.setdefault(package_id, []).append(
                SelectionExclusion(
                    symbol=item["symbol"],
                    score=float(item["score"]),
                    rank=int(item["raw_rank"]),
                    reason=item["reason"],
                    source=item["source"],
                    context=item["context"] or {},
                )
            )
        stored_package_ids = row.get("package_ids") or []
        package_ids = list(stored_package_ids) if stored_package_ids else sorted(set(package_results) | set(excluded_results))
        return SelectionRun(
            run_id=row["run_id"],
            mode=SelectionMode(row["mode"]),
            trade_date=row["trade_date"],
            data_source=row["data_source"],
            package_ids=package_ids,
            runtime_config=row["runtime_config"] or {},
            status=SelectionRunStatus(row["status"]),
            package_results=package_results,
            aggregate_results=aggregate,
            excluded_results=excluded_results,
            manifest_sha256_by_package=manifest_sha,
            valid_no_candidate=bool(row["valid_no_candidate"]),
            no_candidate_reason=row["no_candidate_reason"],
            error=row["error_json"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )

    def create_paper_portfolio_link(self, link: SelectionPaperPortfolioLink) -> SelectionPaperPortfolioLink:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO selection.paper_portfolio_link (
                        run_id, portfolio_id, package_id, manifest_sha256, trade_date,
                        data_source, start_date, initial_cash, runtime_config, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING link_id
                    """,
                    (
                        link.run_id,
                        link.portfolio_id,
                        link.package_id,
                        link.manifest_sha256,
                        link.trade_date,
                        link.data_source,
                        link.start_date,
                        link.initial_cash,
                        psycopg2.extras.Json(link.runtime_config),
                        link.created_at,
                    ),
                )
                row = cur.fetchone()
        return link.model_copy(update={"link_id": int(row[0]) if row else None})

    def list_paper_portfolio_links(self, run_id: str) -> list[SelectionPaperPortfolioLink]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT link_id, run_id, portfolio_id, package_id, manifest_sha256,
                           trade_date, data_source, start_date, initial_cash,
                           runtime_config, created_at
                    FROM selection.paper_portfolio_link
                    WHERE run_id = %s
                    ORDER BY created_at ASC, link_id ASC
                    """,
                    (run_id,),
                )
                rows = cur.fetchall()
        return [
            SelectionPaperPortfolioLink(
                link_id=int(row["link_id"]),
                run_id=row["run_id"],
                portfolio_id=row["portfolio_id"],
                package_id=row["package_id"],
                manifest_sha256=row["manifest_sha256"],
                trade_date=row["trade_date"],
                data_source=row["data_source"],
                start_date=row["start_date"],
                initial_cash=float(row["initial_cash"]),
                runtime_config=row["runtime_config"] or {},
                created_at=row["created_at"],
            )
            for row in rows
        ]


class InMemorySelectionCenterRepository:
    def __init__(self) -> None:
        self.runs: dict[str, SelectionRun] = {}
        self.paper_links: list[SelectionPaperPortfolioLink] = []

    def create_run(self, run: SelectionRun) -> SelectionRun:
        self.runs[run.run_id] = run
        return run

    def complete_run(self, run: SelectionRun) -> SelectionRun:
        completed = run.model_copy(
            update={
                "status": SelectionRunStatus.VALID_NO_CANDIDATE if run.valid_no_candidate else SelectionRunStatus.SUCCEEDED,
                "completed_at": datetime.now(UTC),
            }
        )
        self.runs[completed.run_id] = completed
        return completed

    def fail_run(self, run: SelectionRun, error: dict[str, Any]) -> SelectionRun:
        failed = run.model_copy(
            update={"status": SelectionRunStatus.FAILED, "error": error, "completed_at": datetime.now(UTC)}
        )
        self.runs[failed.run_id] = failed
        return failed

    def list_runs(self, *, limit: int = 100) -> list[SelectionRun]:
        rows = list(self.runs.values())
        rows.sort(key=lambda item: item.created_at, reverse=True)
        return rows[:limit]

    def get_run(self, run_id: str) -> SelectionRun:
        run = self.runs.get(run_id)
        if run is None:
            raise DataUnavailableError("selection run does not exist", context={"run_id": run_id})
        return run

    def create_paper_portfolio_link(self, link: SelectionPaperPortfolioLink) -> SelectionPaperPortfolioLink:
        stored = link.model_copy(update={"link_id": len(self.paper_links) + 1})
        self.paper_links.append(stored)
        return stored

    def list_paper_portfolio_links(self, run_id: str) -> list[SelectionPaperPortfolioLink]:
        return [link for link in self.paper_links if link.run_id == run_id]
